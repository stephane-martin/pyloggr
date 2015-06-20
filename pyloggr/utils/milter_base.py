# encoding: utf-8

# Copyright 2008 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Pure python milter interface, does not use libmilter.

Handles parsing of milter protocol data and provides standard arguments to the callbacks
in your handler class.

http://cpansearch.perl.org/src/AVAR/Sendmail-PMilter-1.00/doc/milter-protocol.txt
https://github.com/avar/sendmail-pmilter/blob/master/doc/milter-protocol.txt
"""

from __future__ import absolute_import, division, print_function
__author__ = 'Eric DeFriez, Stephane Martin'

import logging
import struct

from future.utils import viewitems
from future.builtins import int as long_int


MILTER_VERSION = 2  # Milter version we claim to speak (from pmilter)

# Potential milter command codes and their corresponding MilterBase callbacks.
# From sendmail's include/libmilter/mfdef.h
SMFIC_ABORT   = b'A'   # "Abort"
SMFIC_BODY    = b'B'   # "Body chunk"
SMFIC_CONNECT = b'C'   # "Connection information"
SMFIC_MACRO   = b'D'   # "Define macro"
SMFIC_BODYEOB = b'E'   # "final body chunk (End)"
SMFIC_HELO    = b'H'   # "HELO/EHLO"
SMFIC_HEADER  = b'L'   # "Header"
SMFIC_MAIL    = b'M'   # "MAIL from"
SMFIC_EOH     = b'N'   # "EOH"
SMFIC_OPTNEG  = b'O'   # "Option negotation"
SMFIC_RCPT    = b'R'   # "RCPT to"
SMFIC_QUIT    = b'Q'   # "QUIT"
SMFIC_DATA    = b'T'   # "DATA"
SMFIC_UNKNOWN = b'U'   # "Any unknown command"

COMMANDS = {
    SMFIC_ABORT:   b'abort',
    SMFIC_BODY:    b'body',
    SMFIC_CONNECT: b'connect',
    SMFIC_MACRO:   b'macro',
    SMFIC_BODYEOB: b'end_body',
    SMFIC_HELO:    b'helo',
    SMFIC_HEADER:  b'header',
    SMFIC_MAIL:    b'mail_from',
    SMFIC_EOH:     b'end_headers',
    SMFIC_OPTNEG:  b'opt_neg',
    SMFIC_RCPT:    b'rcpt_to',
    SMFIC_QUIT:    b'quit',
    SMFIC_DATA:    b'data',
    SMFIC_UNKNOWN: b'unknown',
}

# To register/mask callbacks during milter protocol negotiation with sendmail.
# From sendmail's include/libmilter/mfdef.h
NO_CALLBACKS = 127  # (all seven callback flags set: 1111111)
CALLBACKS = {
    'on_connect':       long_int(0x00000001),  # SMFIP_NOCONNECT # Skip SMFIC_CONNECT
    'on_helo':          long_int(0x00000002),  # SMFIP_NOHELO    # Skip SMFIC_HELO
    'on_mail_from':     long_int(0x00000004),  # SMFIP_NOMAIL    # Skip SMFIC_MAIL
    'on_rcpt_to':       long_int(0x00000008),  # SMFIP_NORCPT    # Skip SMFIC_RCPT
    'on_data':          long_int(0x00000200),  # SMFIP_NODATA    # Skip SMFIC_DATA
    'on_header':        long_int(0x00000020),  # SMFIP_NOHDRS    # Skip SMFIC_HEADER
    'on_end_headers':   long_int(0x00000040),  # SMFIP_NOEOH     # Skip SMFIC_EOH
    'on_body':          long_int(0x00000010),  # SMFIP_NOBODY    # Skip SMFIC_BODY
}

# Acceptable response commands/codes to return to sendmail (with accompanying
# command data).  From sendmail's include/libmilter/mfdef.h
RESPONSE = {
    'ADDRCPT':    b'+',  # SMFIR_ADDRCPT    # "add recipient"
    'DELRCPT':    b'-',  # SMFIR_DELRCPT    # "remove recipient"
    'ACCEPT':     b'a',  # SMFIR_ACCEPT     # "accept"
    'REPLBODY':   b'b',  # SMFIR_REPLBODY   # "replace body (chunk)"
    'CONTINUE':   b'c',  # SMFIR_CONTINUE   # "continue"
    'DISCARD':    b'd',  # SMFIR_DISCARD    # "discard"
    'CONNFAIL':   b'f',  # SMFIR_CONN_FAIL  # "cause a connection failure"
    'ADDHEADER':  b'h',  # SMFIR_ADDHEADER  # "add header"
    'INSHEADER':  b'i',  # SMFIR_INSHEADER  # "insert header"
    'CHGHEADER':  b'm',  # SMFIR_CHGHEADER  # "change header"
    'PROGRESS':   b'p',  # SMFIR_PROGRESS   # "progress"
    'QUARANTINE': b'q',  # SMFIR_QUARANTINE # "quarantine"
    'REJECT':     b'r',  # SMFIR_REJECT     # "reject"
    'SETSENDER':  b's',  # v3 only?
    'TEMPFAIL':   b't',  # SMFIR_TEMPFAIL   # "tempfail"
    'REPLYCODE':  b'y',  # SMFIR_REPLYCODE  # "reply code etc"
}


def canonicalize_address(addr):
    """
    Strip angle brackes from email address iff not an empty address ("<>").

    :param addr: the email address to canonicalize (strip angle brackets from).
    :returns: the addr with leading and trailing angle brackets
    """
    if addr == b'<>':
        return addr
    return addr.lstrip(b'<').rstrip(b'>')


class MilterException(Exception):
    """
    Parent of all other MilterBase exceptions.

    Note
    ====
    Subclass this, do not construct or catch explicitly!
    """


class MilterPermFailure(MilterException):
    """
    Milter exception that indicates a perment failure
    """


class MilterTempFailure(MilterException):
    """
    Milter exception that indicates a temporary/transient failure.
    """


class MilterCloseConnection(MilterException):
    """
    Exception that indicates the server should close the milter connection
    """


class MilterActionError(MilterException):
    """
    Exception raised when an action is performed that was not negotiated
    """


class MilterDispatcher(object):
    """
    Dispatcher class for a milter server.  This class accepts entire
    milter commands as a string (command character + binary data), parses
    the command and binary data appropriately and invokes the appropriate
    callback function in a milter_class instance. One MilterDispatcher
    per socket connection.  One milter_class instance per MilterDispatcher
    (per socket connection).

    Args:
        milter_class: A class (not an instance) that handles callbacks for milter
        commands (e.g. a child of the MilterBase class).
    """

    def __init__(self, milter_class, context=None):
        if context is not None:
            self.milter_obj = milter_class(context)
        else:
            self.milter_obj = milter_class()

    def dispatch(self, data):
        """
        Callback function for the milter socket server to handle a single
        milter command.  Parses the milter command data, invokes the milter
        handler, and formats a suitable response for the server to send
        on the socket.


        :param data: A binary string consisting of a command code character
        followed by binary data for that command code.

        :returns: a binary string to write on the socket and return to sendmail; the
        string typically consists of a RESPONSE[] command character then
        some response-specific protocol data.

        :raises MilterCloseConnection: Indicating the milter connection should be closed.
        """
        logger = logging.getLogger(__name__)
        (cmd, data) = (data[0], data[1:])
        try:
            if cmd not in COMMANDS:
                logger.warn('milter_base: unknown command code: "%s" ("%s")', cmd, data)
                return RESPONSE['CONTINUE']
            command = COMMANDS[cmd]
            logger.debug('milter_base: command: %s', command)
            parser_callback_name = '_parse_%s' % command
            handler_callback_name = 'on_%s' % command

            if not hasattr(self, parser_callback_name):
                logger.error('No parser implemented for "%s"', command)
                return RESPONSE['CONTINUE']

            if not hasattr(self.milter_obj, handler_callback_name):
                logger.warn('Unimplemented command: "%s" ("%s")', command, data)
                return RESPONSE['CONTINUE']

            parser = getattr(self, parser_callback_name)
            callback = getattr(self.milter_obj, handler_callback_name)
            args = parser(cmd, data)
            return callback(*args)
        except MilterTempFailure as e:
            logger.info('Temp Failure: %s', str(e))
            return RESPONSE['TEMPFAIL']
        except MilterPermFailure as e:
            logger.info('Perm Failure: %s', str(e))
            return RESPONSE['REJECT']

    # noinspection PyMethodMayBeStatic
    def _parse_opt_neg(self, cmd, data):
        """
        Parse the 'OptNeg' milter data into arguments for the milter handler.

        :param cmd: A single character command code representing this command.
        :param data: Command-specific milter data to be unpacked/parsed.

        :returns:
          A tuple consisting of:
            cmd: The single character command code representing this command.
            ver: The protocol version we support.
            actions: Bitmask of the milter actions we may perform
                     (see "MilterBase.ACTION_*").
            protocol: Bitmask of the callback functions we are registering.

        """
        (ver, actions, protocol) = struct.unpack('!III', data)
        return cmd, ver, actions, protocol

    # noinspection PyMethodMayBeStatic
    def _parse_macro(self, cmd, data):
        """
        Parse the 'Macro' milter data into arguments for the milter handler.

        :param cmd: A single character command code representing this command.
        :param data: Command-specific milter data to be unpacked/parsed.

        :returns:
          A tuple consisting of:
            cmd: The single character command code representing this command.
            macro: The single character command code this macro is for.
            data: A list of strings alternating between name, value of macro.
        """
        (macro, data) = (data[0], data[1:])
        return cmd, macro, data.split('\0')

    # noinspection PyMethodMayBeStatic
    def _parse_connect(self, cmd, data):
        """
        Parse the 'Connect' milter data into arguments for the milter handler.

        :param cmd: A single character command code representing this command.
        :param data: Command-specific milter data to be unpacked/parsed.

        Returns:
          A tuple (cmd, hostname, family, port, address) where:
            cmd: The single character command code representing this command.
            hostname: The hostname that originated the connection to the MTA.
            family: Address family for connection (see sendmail libmilter/mfdef.h).
            port: The network port if appropriate for the connection.
            address: Remote address of the connection (e.g. IP address).
        """
        (hostname, data) = data.split('\0', 1)
        family = struct.unpack('c', data[0])[0]
        port = struct.unpack('!H', data[1:3])[0]
        address = data[3:]
        return cmd, hostname, family, port, address

    # noinspection PyMethodMayBeStatic
    def _parse_helo(self, cmd, data):
        """
        Parse the 'Helo' milter data into arguments for the milter handler.

        :param cmd: A single character command code representing this command.
        :param data: Command-specific milter data to be unpacked/parsed.

        :returns:
          A tuple (cmd, data) where:
            cmd: The single character command code representing this command.
            data: TODO: parse this better
        """
        return cmd, data

    # noinspection PyMethodMayBeStatic
    def _parse_mail_from(self, cmd, data):
        """
        Parse the 'MailFrom' milter data into arguments for the milter handler.

        :param cmd: A single character command code representing this command.
        :param data: Command-specific milter data to be unpacked/parsed.

        :returns:
          A tuple (cmd, mailfrom, esmtp_info) where:
            cmd: The single character command code representing this command.
            mailfrom: The canonicalized MAIL From email address.
            esmtp_info: Extended SMTP (esmtp) info as a list of strings.
        """
        (mailfrom, esmtp_info) = data.split('\0', 1)
        return cmd, canonicalize_address(mailfrom), esmtp_info.split('\0')

    # noinspection PyMethodMayBeStatic
    def _parse_rcpt_to(self, cmd, data):
        """
        Parse the 'RcptTo' milter data into arguments for the milter handler.

        :param cmd: A single character command code representing this command.
        :param data: Command-specific milter data to be unpacked/parsed.

        :returns:
          A tuple (cmd, rcptto, emstp_info) where:
            cmd: The single character command code representing this command.
            rcptto: The canonicalized RCPT To email address.
            esmtp_info: Extended SMTP (esmtp) info as a list of strings.
        """
        (rcptto, esmtp_info) = data.split('\0', 1)
        return cmd, canonicalize_address(rcptto), esmtp_info.split('\0')

    # noinspection PyMethodMayBeStatic
    def _parse_header(self, cmd, data):
        """
        Parse the 'Header' milter data into arguments for the milter handler.

        :param cmd: A single character command code representing this command.
        :param data: Command-specific milter data to be unpacked/parsed.

        :returns:
          A tuple (cmd, key, val) where:
            cmd: The single character command code representing this command.
            key: The name of the header.
            val: The value/data for the header.
        """
        (key, val) = data.split('\0', 1)
        val, _ = val.split('\0', 1)
        return cmd, key, val

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def _parse_end_headers(self, cmd, data):
        """
        Parse the 'EndHeaders' milter data into arguments for the milter handler.

        :param cmd: A single character command code representing this command.
        :param data: Command-specific milter data to be unpacked/parsed.

        :returns:
          A tuple (cmd) where:
            cmd: The single character command code representing this command.
        """
        return cmd,

    # noinspection PyMethodMayBeStatic
    def _parse_body(self, cmd, data):
        """Parse the 'Body' milter data into arguments for the milter handler.

        :param cmd: A single character command code representing this command.
        :param data: Command-specific milter data to be unpacked/parsed.

        :returns:
          A tuple (cmd, data) where:
            cmd : The single character command code representing this command.
            data: TODO: parse this better
        """
        return cmd, data

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def _parse_end_body(self, cmd, data):
        """
        Parse the 'EndBody' milter data into arguments for the milter handler.

        :param cmd: A single character command code representing this command.
        :param data: No data is sent for this command.

        :returns:
          A tuple (cmd) where:
            cmd: The single character command code representing this command.
        """
        return cmd,

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def _parse_quit(self, cmd, data):
        """
        Parse the 'Quit' milter data into arguments for the milter handler.

        :param cmd: A single character command code representing this command.
        :param data: Command-specific milter data to be unpacked/parsed.

        :returns:
          A tuple (cmd) where:
            cmd: The single character command code representing this command.
        """
        return cmd,

    # noinspection PyMethodMayBeStatic,PyUnusedLocal
    def _parse_abort(self, cmd, data):
        """
        Parse the 'Abort' milter data into arguments for the milter handler.

        :param cmd: A single character command code representing this command.
        :param data: Command-specific milter data to be unpacked/parsed.

        :returns: A tuple (cmd,) where cmd is rhe single character command code representing this command
        """
        return cmd,


class MilterBase(object):
    """
    Pure python milter handler base class.

    Inherit from this class and override any On*() commands you would like your milter to handle.
    Register any actions your milter may perform using the can_*() functions
    during your __init__() to ensure your milter's actions are accepted.
    """

    # Actions we tell sendmail we may perform
    # MilterBase users invoke self.CanFoo() during their __init__()
    # to toggle these settings.
    ACTION_ADDHDRS    = 1   # 0x01 SMFIF_ADDHDRS    # Add headers
    ACTION_CHGBODY    = 2   # 0x02 SMFIF_CHGBODY    # Change body chunks
    ACTION_ADDRCPT    = 4   # 0x04 SMFIF_ADDRCPT    # Add recipients
    ACTION_DELRCPT    = 8   # 0x08 SMFIF_DELRCPT    # Remove recipients
    ACTION_CHGHDRS    = 16  # 0x10 SMFIF_CHGHDRS    # Change or delete headers
    ACTION_QUARANTINE = 32  # 0x20 SMFIF_QUARANTINE # Quarantine message

    def __init__(self):
        self._actions = 0
        self._protocol = NO_CALLBACKS
        for (callback, flag) in viewitems(CALLBACKS):
            if hasattr(self, callback):
                self._protocol &= ~flag

    # noinspection PyMethodMayBeStatic
    def accept(self):
        """
        Create an 'ACCEPT' response to return to the milter dispatcher.
        """
        return RESPONSE['ACCEPT']

    # noinspection PyMethodMayBeStatic
    def reject(self):
        """
        Create a 'REJECT' response to return to the milter dispatcher.
        """
        return RESPONSE['REJECT']

    # noinspection PyMethodMayBeStatic
    def discard(self):
        """
        Create a 'DISCARD' response to return to the milter dispatcher.
        """
        return RESPONSE['DISCARD']

    # noinspection PyMethodMayBeStatic
    def tempfail(self):
        """
        Create a 'TEMPFAIL' response to return to the milter dispatcher.
        """
        return RESPONSE['TEMPFAIL']

    # noinspection PyMethodMayBeStatic
    def response_continue(self):
        """
        Create an '' response to return to the milter dispatcher.
        """
        return RESPONSE['CONTINUE']

    # noinspection PyMethodMayBeStatic
    def custom_reply(self, code, text):
        """
        Create a 'REPLYCODE' (custom) response to return to the milter
        dispatcher.

        :param code: integer or digit string (should be \d\d\d)
        :param text: code reason/explanation to send to the user

        Note
        ====
        A '421' reply code will cause sendmail to close the connection after responding!

        """
        return '%s%s %s\0' % (RESPONSE['REPLYCODE'], code, text)

    def add_recipient(self, rcpt):
        """
        Construct an ADDRCPT reply that the client can send during OnEndBody.

        :param rcpt: The recipient to add, should have <> around it.
        """
        self._verify_capability(self.ACTION_ADDRCPT)
        return '%s%s\0' % (RESPONSE['ADDRCPT'], rcpt)

    def add_header(self, name, value):
        """
        Construct an ADDHEADER reply that the client can send during OnEndBody.

        :param name: The name of the header to add
        :param value: The value of the header
        """
        self._verify_capability(self.ACTION_ADDHDRS)
        return '%s%s\0%s\0' % (RESPONSE['ADDHEADER'], name, value)

    def delete_recipient(self, rcpt):
        """
        Construct an DELRCPT reply that the client can send during OnEndBody.

        :param rcpt: The recipient to delete, should have <> around it.
        """
        self._verify_capability(self.ACTION_DELRCPT)
        return '%s%s\0' % (RESPONSE['DELRCPT'], rcpt)

    def insert_header(self, index, name, value):
        """
        Construct an INSHEADER reply that the client can send during OnEndBody.

        :param index: The index to insert the header at. 0 is above all headers.
        A number greater than the number of headers just appends.

        :param name: The name of the header to insert
        :param value: The value to insert
        """
        self._verify_capability(self.ACTION_ADDHDRS)
        index = struct.pack('!I', index)
        return '%s%s%s\0%s\0' % (RESPONSE['INSHEADER'], index, name, value)

    def change_header(self, index, name, value):
        """
        Construct a CHGHEADER reply that the client can send during OnEndBody.

        :param index: The index of the header to change, offset from 1.
        The offset is per-occurance of this header, not of all headers.
        A value of '' (empty string) will cause the header to be deleted.

        :param name: The name of the header to insert.
        :param value: The value to insert.
        """
        self._verify_capability(self.ACTION_CHGHDRS)
        index = struct.pack('!I', index)
        return '%s%s%s\0%s\0' % (RESPONSE['CHGHEADER'], index, name, value)

    def return_on_end_body_actions(self, actions):
        """
        Construct an OnEndBody response that can consist of multiple actions
        followed by a final required Continue().

        All message mutations (all adds/changes/deletes to envelope/header/body)
        must be sent as response to the OnEndBody callback.  Multiple actions
        are allowed.  This function formats those multiple actions into one
        response to return back to the MilterDispatcher.

        For example to make sure all recipients are in 'To' headers:
        +---------------------------------------------------------------------
        | class NoBccMilter(PpyMilterBase):
        |  def __init__(self):
        |    self.__mutations = []
        |    ...
        |  def OnRcptTo(self, cmd, rcpt_to, esmtp_info):
        |    self.__mutations.append(self.AddHeader('To', rcpt_to))
        |    return self.Continue()
        |  def OnEndBody(self, cmd):
        |    tmp = self.__mutations
        |    self.__mutations = []
        |    return self.ReturnOnEndBodyActions(tmp)
        |  def OnResetState(self):
        |    self.__mutations = []
        +---------------------------------------------------------------------


        :param actions: List of "actions" to perform on the message. For example:
        actions = [AddHeader('Cc', 'lurker@example.com'), AddRecipient('lurker@example.com')]
        """
        return actions[:] + [self.response_continue()]

    def _reset_state(self):
        """
        Clear out any per-message data.

        Milter connections correspond to SMTP connections, and many messages may be
        sent in the same SMTP conversation. Any data stored that pertains to the
        message that was just handled should be cleared so that it doesn't affect
        processing of the next message. This method also implements an
        'OnResetState' callback that milters can use to catch this situation too.
        """
        try:
            self.on_reset_state()
        except NotImplementedError:
            logging.getLogger(__name__).warn('No OnResetState() callback is defined for this milter.')

    def on_reset_state(self):
        """
        Callback that can be used when the previous message data should be cleared
        """
        raise NotImplementedError

    def on_opt_neg(self, cmd, ver, actions, protocol):
        """
        Callback for the 'OptNeg' (option negotiation) milter command.

        Shouldn't be necessary to override (don't do it unless you
        know what you're doing).

        Option negotation is based on:

        - Command callback functions defined by your handler class
        - Stated actions your milter may perform by invoking the  "self.can_foo()"
        functions during your milter's __init__().

        """
        out = struct.pack('!III', MILTER_VERSION,
                          self._actions & actions,
                          self._protocol & protocol)
        return cmd + out

    # noinspection PyMethodMayBeStatic
    def on_macro(self, cmd, macro_cmd, data):
        """
        Callback for the 'Macro' milter command: no response required
        """
        return None

    # noinspection PyMethodMayBeStatic
    def on_quit(self, cmd):
        """
        Callback for the 'Quit' milter command: close the milter connection.

        The only logical response is to ultimately raise a
        MilterCloseConnection() exception.
        """
        raise MilterCloseConnection('received quit command')

    # noinspection PyUnusedLocal
    def on_abort(self, cmd):
        """
        Callback for the 'Abort' milter command.

        This callback is required because per-message data must be cleared when an
        Abort command is received. Otherwise any message modifications will end up
        being applied to the next message that is sent down the same SMTP
        connection.

        :param cmd: unused argument.

        :returns: a Continue response so that further messages in this SMTP conversation
        will be processed.
        """
        logging.getLogger(__name__).debug("milter_base: received abort from Postfix")
        self._reset_state()
        return self.response_continue()

    def on_end_body(self, cmd):
        """
        Callback for the 'EndBody' milter command.

        If your milter wants to do any message mutations (add/change/delete any
        envelope/header/body information) it needs to happen as a response to
        this callback (so need to override this function and cause those
        actions by returning using ReturnOnEndBodyActions() above).

        :param cmd: Unused argument.

        :returns: a continue response so that further messages in this SMTP conversation
        will be processed.
        """
        return self.response_continue()

    # Call these can_* methods from __init__() to tell sendmail you may perform these actions
    def can_add_headers(self):
        """
        Register that our milter may perform the action 'ADDHDRS'
        """
        self._actions |= self.ACTION_ADDHDRS

    def can_change_body(self):
        """
        Register that our milter may perform the action 'CHGBODY'
        """
        self._actions |= self.ACTION_CHGBODY

    def can_add_recipient(self):
        """
        Register that our milter may perform the action 'ADDRCPT'
        """
        self._actions |= self.ACTION_ADDRCPT

    def can_delete_recipient(self):
        """
        Register that our milter may perform the action 'DELRCPT'
        """
        self._actions |= self.ACTION_DELRCPT

    def can_change_headers(self):
        """
        Register that our milter may perform the action 'CHGHDRS'
        """
        self._actions |= self.ACTION_CHGHDRS

    def can_quarantine(self):
        """
        Register that our milter may perform the action 'QUARANTINE'
        """
        self._actions |= self.ACTION_QUARANTINE

    def _verify_capability(self, action):
        if not (self._actions & action):
            logging.getLogger(__name__).error('Error: Attempted to perform an action that was not requested')
            raise MilterActionError('Action not requested in __init__')
