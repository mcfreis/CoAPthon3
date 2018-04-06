# -*- coding: utf-8 -*-

import time
import logging
import threading
from coapthon import defines
from coapthon.transaction import Transaction
from coapthon.messages.message import Message
from coapthon.messages.request import Request
from coapthon.messages.response import Response

logger = logging.getLogger(__name__)

__author__ = 'Björn Freise'


class PayloadBlock(object):
    def __init__(self, pl=None, ct=None, bo=(0, 0, 0)):
        self.payload = pl
        self.content_type = ct
        self._bo = bo
        self._ack = None

    @property
    def acked(self):
        return self._ack

    @acked.setter
    def acked(self, value):
        self._ack = value

    @property
    def block_options(self):
        return self._bo

    def __str__(self):
        return self.payload if self.payload is not None else ''


class BlockItem(object):
    def __init__(self, bo_num, bo_m, bo_size, payload=None, content_type=None, timeout=None):
        """
        Data structure to store Block parameters

        :param int bo_num: the num field of the block option
        :param int bo_m: the M bit of the block option
        :param int bo_size: the size field of the block option
        :param str | None payload: the overall payload received in all blocks
        :param int | None content_type: the content-type of the payload
        """
        self.bo_num = bo_num
        self.bo_m = bo_m
        self.bo_size = bo_size
        self.content_type = content_type

        self._creation_time = time.time()
        self._last_access = None
        self._timeout = timeout
        self._update_timestamp()

        self._total_size = 0 if payload is None else len(payload)
        self._partial_payload = list()  # type: list[PayloadBlock]

        if self._total_size > 0:
            self._fill_partial_payload(payload)

    def _fill_partial_payload(self, payload=None):
        self._partial_payload = [PayloadBlock()] * ((self._total_size + self.bo_size - 1) // self.bo_size)
        for i in range(0, self._total_size, self.bo_size):
            num = i // self.bo_size
            m = 1 if (num + 1 < len(self._partial_payload)) else 0
            part = payload[i:i + self.bo_size] if payload is not None else None
            self._partial_payload[num] = PayloadBlock(part, self.content_type, (num, m, self.bo_size))

    def __setitem__(self, i, o):
        self._update_timestamp()
        self._partial_payload[i] = o

    def __getitem__(self, i):
        self._update_timestamp()
        return self._partial_payload[i]

    @property
    def duration(self):
        return self._last_access - self._creation_time

    @property
    def payload(self):
        pl = ''.join([str(x) for x in self._partial_payload])
        if len(pl) > 0:
            return pl
        return None

    @property
    def block_options(self):
        return self.bo_num, self.bo_m, self.bo_size

    @block_options.setter
    def block_options(self, value):
        num, m, size = value
        if size != self.bo_size:
            payload = self.payload
            self.bo_size = size
            self._fill_partial_payload(payload)
        self.bo_num = num
        self.bo_m = m

    @property
    def total_size(self):
        return self._total_size

    @total_size.setter
    def total_size(self, value):
        self._total_size = value
        self._fill_partial_payload(None)

    @property
    def complete(self):
        for part in self._partial_payload:
            if part.acked is not True:
                return False
        return True

    def next_block_options(self):
        if self.complete is False:
            while self._partial_payload[self.bo_num].acked is True:
                self.bo_num = (self.bo_num + 1) % len(self._partial_payload)
        self.bo_m = self._partial_payload[self.bo_num].block_options[1]
        return self.block_options

    def _update_timestamp(self):
        self._last_access = time.time()

    def expired(self):
        if self._timeout is None:
            return False
        else:
            return (time.time() - self._last_access) > self._timeout


class BlockLayer(object):
    """
    Handle the Blockwise options. Hides all the exchange to both servers and clients and keeps a buffer of the data
    to be exchanged for each transaction.
    """

    class BlockDict(dict):

        def __init__(self, **kwargs):
            super(BlockLayer.BlockDict, self).__init__(**kwargs)
            self._lock = threading.RLock()

        def __enter__(self):
            self._lock.acquire()

        def __exit__(self, exc_type, exc_val, exc_tb):
            self._lock.release()

    def __init__(self):
        self._session_timeout = 3 * 60  # 3 Minutes
        self._block1_sent = {}  # type: dict[int, BlockItem]
        self._block2_sent = {}  # type: dict[int, BlockItem]
        self._block1_receive = {}  # type: dict[int, BlockItem]
        self._block2_receive = self.BlockDict()  # type: dict[int, BlockItem]

    def _clean_session(self):
        for session in [self._block1_sent, self._block2_sent, self._block1_receive, self._block2_receive]:
            for k, v in session.items():
                if v.expired():
                    del session[k]

    def receive_request(self, transaction):
        """
        Handles the Blocks option in a incoming request.

        Direction:  client -> server
        Action on:  server-side
        Options:    with GET:           block2 (client can indicate its desired block size)
                    with PUT or POST:   block1 (information about the contained payload)

        :param Transaction transaction: the transaction that owns the request
        :rtype: Transaction
        :return: the edited transaction
        """
        self._clean_session()

        host, port = transaction.request.source
        key_token = hash(str(host) + str(port) + str(transaction.request.token))
        transaction.block_transfer = False

        if transaction.request.block2 is not None:
            # GET
            num, m, size = transaction.request.block2
            del transaction.request.block2

            if key_token in self._block2_receive:
                while not self._block2_receive[key_token].total_size:
                    time.sleep(0.5)

            with self._block2_receive:
                if key_token in self._block2_receive:
                    # n-th request, update block options
                    self._block2_receive[key_token].block_options = num, m, size

                    # Add buffered payload
                    transaction.block_transfer = True
                    transaction.response = Response()
                    transaction.response.destination = transaction.request.source
                    transaction.response.token = transaction.request.token
                    transaction.response.code = defines.Codes.CONTENT.number

                    del transaction.response.block2
                    try:
                        transaction.response.payload = self._block2_receive[key_token][num].payload
                        transaction.response.block2 = self._block2_receive[key_token][num].block_options
                    except IndexError:
                        transaction.response.payload = None
                        transaction.response.block2 = (num, m, size)

                    del transaction.response.size2
                    transaction.response.size2 = self._block2_receive[key_token].total_size
                    if transaction.request.content_type != self._block2_receive[key_token][num].content_type:
                        try:
                            transaction.response.content_type = self._block2_receive[key_token][num].content_type
                        except:
                            pass

                else:
                    # early negotiation
                    self._block2_receive[key_token] = BlockItem(num, m, size, timeout=self._session_timeout)

                    # Payload is not available yet ... in send_response() it will be added.

        elif transaction.request.block1 is not None:
            # POST or PUT
            num, m, size = transaction.request.block1
            del transaction.request.block1

            if transaction.request.size1 is not None:
                # TODO: What to do if the size1 is larger than the maximum resource size or the maximum server buffer
                pass

            if key_token in self._block1_receive:
                # n-th block
                if transaction.request.content_type != self._block1_receive[key_token].content_type:
                    # Error Incomplete
                    return self._error(transaction, defines.Codes.UNSUPPORTED_CONTENT_FORMAT.number)

            else:
                # first block
                self._block1_receive[key_token] = BlockItem(num, m, size,
                                                            None, transaction.request.content_type,
                                                            timeout=self._session_timeout)

            if transaction.request.size1 is not None and \
               transaction.request.size1 != self._block1_receive[key_token].total_size:
                self._block1_receive[key_token].total_size = transaction.request.size1

            self._block1_receive[key_token][num] = PayloadBlock(transaction.request.payload,
                                                                transaction.request.content_type,
                                                                (num, m, size))
            self._block1_receive[key_token][num].acked = True

            if self._block1_receive[key_token].complete:
                transaction.request.payload = self._block1_receive[key_token].payload
                transaction.request.duration = self._block1_receive[key_token].duration
                # end of blockwise
                del self._block1_receive[key_token]
            else:
                # Continue
                transaction.block_transfer = True
                transaction.response = Response()
                transaction.response.destination = transaction.request.source
                transaction.response.token = transaction.request.token
                transaction.response.code = defines.Codes.CONTINUE.number
                transaction.response.block1 = (num, m, size)

        return transaction

    def receive_response(self, transaction):
        """
        Handles the Blocks option in a incoming response.

        Direction:  server -> client
        Action on:  client-side
        Options:    with GET:           block2 (information about the contained payload)
                    with PUT or POST:   block1 (indicates the block size preference of the server)

        :param Transaction transaction: the transaction that owns the response
        :rtype: Transaction
        :return: the edited transaction
        """
        self._clean_session()

        host, port = transaction.response.source
        key_token = hash(str(host) + str(port) + str(transaction.response.token))
        transaction.block_transfer = False

        if transaction.response.block2 is not None:
            # GET
            num, m, size = transaction.response.block2

            if key_token in self._block2_sent and self._block2_sent[key_token].total_size > 0:
                # n-th block
                if transaction.response.content_type != self._block2_sent[key_token].content_type:
                    # Error Incomplete
                    return self._error(transaction, defines.Codes.UNSUPPORTED_CONTENT_FORMAT.number)

            else:
                self._block2_sent[key_token] = BlockItem(num, m, size,
                                                         None, transaction.response.content_type,
                                                         timeout=self._session_timeout)

            if transaction.response.size2 is not None and \
               transaction.response.size2 != self._block2_sent[key_token].total_size:
                self._block2_sent[key_token].total_size = transaction.response.size2

            self._block2_sent[key_token][num] = PayloadBlock(transaction.response.payload,
                                                             transaction.response.content_type, (num, m, size))
            self._block2_sent[key_token][num].acked = True
            self._block2_sent[key_token].block_options = num, m, size

            if self._block2_sent[key_token].complete:
                transaction.response.payload = self._block2_sent[key_token].payload
                del self._block2_sent[key_token]

            else:
                transaction.block_transfer = True
                del transaction.request.mid
                del transaction.request.block2
                transaction.request.block2 = self._block2_sent[key_token].next_block_options()

        elif transaction.response.block1 is not None and key_token in self._block1_sent:
            # PUT or POST
            n_num, n_m, n_size = transaction.response.block1
            self._block1_sent[key_token][n_num].acked = True

            del transaction.request.mid

            del transaction.request.block1
            transaction.request.block1 = self._block1_sent[key_token].next_block_options()

            num = self._block1_sent[key_token].block_options[0]
            transaction.request.payload = self._block1_sent[key_token][num].payload

            # The original request already has this option set
            del transaction.request.size1
            transaction.request.size1 = self._block1_sent[key_token].total_size

            if self._block1_sent[key_token].complete:
                del transaction.request.block1

            else:
                transaction.block_transfer = True

        return transaction

    def receive_empty(self, empty, transaction):
        """
        Dummy function. Used to not break the layered architecture.

        :param Message empty: the received empty message
        :param Transaction transaction: the transaction that owns the empty message
        :rtype: Transaction
        :return: the transaction
        """
        return transaction

    def send_response(self, transaction):
        """
        Handles the Blocks option in a outgoing response.

        Direction:  server -> client
        Action on:  server-side
        Options:    with GET:           block2 (information about the contained payload)
                    with PUT or POST:   block1 (indicates the block size preference of the server)

        :param Transaction transaction: the transaction that owns the response
        :rtype: Transaction
        :return: the edited transaction
        """
        self._clean_session()

        host, port = transaction.request.source
        key_token = hash(str(host) + str(port) + str(transaction.request.token))

        with self._block2_receive:
            if (key_token in self._block2_receive and transaction.response.payload is not None) or \
                    (transaction.response.payload is not None and len(transaction.response.payload) > defines.MAX_PAYLOAD):
                # GET
                if key_token in self._block2_receive:
                    # specific call
                    num, m, size = self._block2_receive[key_token].block_options

                else:
                    # unspecific call
                    num, m, size = 0, 1, defines.MAX_PAYLOAD

                self._block2_receive[key_token] = BlockItem(num, m, size,
                                                            transaction.response.payload, transaction.response.content_type,
                                                            timeout=self._session_timeout)

                if (transaction.request.size2 is not None and transaction.request.size2 == 0) or \
                   (transaction.response.payload is not None and len(transaction.response.payload) > defines.MAX_PAYLOAD):
                    del transaction.response.size2
                    transaction.response.size2 = self._block2_receive[key_token].total_size

                del transaction.response.block2
                try:
                    transaction.response.payload = self._block2_receive[key_token][num].payload
                    transaction.response.block2 = self._block2_receive[key_token][num].block_options
                except IndexError:
                    transaction.response.payload = None
                    transaction.response.block2 = (num, m, size)

        return transaction

    def send_request(self, request):
        """
        Handles the Blocks option in a outgoing request.

        Direction:  client -> server
        Action on:  client-side
        Options:    with GET:           block2 (client can indicate its desired block size)
                    with PUT or POST:   block1 (information about the contained payload)

        :param Request request: the outgoing request
        :rtype: Request
        :return: the edited request
        """
        self._clean_session()

        host, port = request.destination
        key_token = hash(str(host) + str(port) + str(request.token))

        if request.block2 is not None:
            # GET
            num, m, size = request.block2
            self._block2_sent[key_token] = BlockItem(num, m, size, timeout=self._session_timeout)

        elif request.block1 is not None or \
                (request.payload is not None and len(request.payload) > defines.MAX_PAYLOAD):
            # PUT or POST
            size = defines.MAX_PAYLOAD
            if request.block1 is not None:
                _num, _m, size = request.block1
            num, m, size = 0, 1, size
            self._block1_sent[key_token] = BlockItem(num, m, size,
                                                     request.payload, request.content_type,
                                                     timeout=self._session_timeout)

            request.payload = self._block1_sent[key_token][num].payload
            del request.block1
            request.block1 = self._block1_sent[key_token][num].block_options
            del request.size1
            request.size1 = self._block1_sent[key_token].total_size

        return request

    @staticmethod
    def _incomplete(transaction):
        """
        Notifies incomplete blockwise exchange.

        :type transaction: Transaction
        :param transaction: the transaction that owns the response
        :rtype: Transaction
        :return: the edited transaction
        """
        transaction.block_transfer = True
        transaction.response = Response()
        transaction.response.destination = transaction.request.source
        transaction.response.token = transaction.request.token
        transaction.response.code = defines.Codes.REQUEST_ENTITY_INCOMPLETE.number
        return transaction

    @staticmethod
    def _error(transaction, code):
        """
        Notifies generic error on blockwise exchange.

        :param code:
        :type transaction: Transaction
        :param transaction: the transaction that owns the response
        :rtype: Transaction
        :return: the edited transaction
        """
        transaction.block_transfer = True
        transaction.response = Response()
        transaction.response.destination = transaction.request.source
        transaction.response.type = defines.Types["RST"]
        transaction.response.token = transaction.request.token
        transaction.response.code = code
        return transaction
