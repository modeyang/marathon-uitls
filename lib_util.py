#!/usr/bin/env python
# coding=utf-8
import config
import logging

logger = logging.getLogger(__name__)


class APIProxy(object):
    """API Object classe

    :type parent: : class object
    :param parent:  object to use as parent.
    """

    def __init__(self, parent):
        self.parent = parent

    def __getattr__(self, name):
        """Dynamically create a method.

        :type name: method name
        """
        if hasattr(self.parent, name) is None:
            logger.error("no method named: " + name)
            raise
        return getattr(self.parent, name)
