#! /usr/bin/env python
#-*- coding: utf-8 -*-

# ==============================================================================
#                                      IMPORTS
# ==============================================================================

from unittest import TestCase

# Services
from graphalchemy.ogm.repository import Query
from graphalchemy.ogm.repository import Repository
from graphalchemy.fixture.declarative import Page
from graphalchemy.fixture.declarative import page
from graphalchemy.fixture.declarative import metadata


# ==============================================================================
#                                     TESTING
# ==============================================================================

class RepositoryTestCase(TestCase):

    def setUp(self):
        from bulbs.titan import TitanClient
        client = TitanClient(db_name="graph")
        from bulbs.rest import log
        log.setLevel(1)
        from graphalchemy.ogm.session import Session
        self.session = Session(client=client, metadata=metadata, logger=log)
        self.repository = Repository(self.session, page, Page, logger=log)


    def test__compile_groovy(self):

        query = Query(self.session)

        self.assertEquals((
            u'g.v(eid)',
            {'eid': 123}
        ), query.vertices().filter(eid=123).compile())

        self.assertEquals((
            u'g.V.has("name", name)',
            {u'name': 'Foo'}
        ), query.vertices().filter(name='Foo').compile())
        self.assertEquals((
            u'g.V.has("content", content).has("name", name)',
            {u'content': 'Bar', u'name': 'Foo'}
        ), query.vertices().filter(name='Foo', content='Bar').compile())
        self.assertEquals((
            u'g.V.has("content", content).has("name", name)',
            {u'content': 'Bar', u'name': 'Foo'}
        ), query.vertices().filter(name='Foo').filter(content='Bar').compile())

        self.assertEquals((
            u'g.V("name", name).has("content", content)',
            {u'content': 'Bar', u'name': 'Foo'}
        ), query.vertices().filter_on_index('name', 'name', 'Foo').filter(content='Bar').compile())

        self.assertEquals((
            u'g.v(eid).has("name", name)',
            {'eid': 123, u'name': 'Foo'}
        ), query.vertices().filter(eid=123).filter(name='Foo').compile())
        self.assertEquals((
            u'g.v(eid).has("name", name)',
            {'eid': 123, u'name': 'Foo'}
        ), query.vertices().filter(eid=123, name='Foo').compile())


        query = Query(self.session)

        self.assertEquals((
            u'g.e(eid)',
            {'eid': 123}
        ), query.edges().filter(eid=123).compile())

        self.assertEquals((
            u'g.E.has("name", name)',
            {u'name': 'Foo'}
        ), query.edges().filter(name='Foo').compile())
        self.assertEquals((
            u'g.E.has("content", content).has("name", name)',
            {u'content': 'Bar', u'name': 'Foo'}
        ), query.edges().filter(name='Foo', content='Bar').compile())
        self.assertEquals((
            u'g.E.has("content", content).has("name", name)',
            {u'content': 'Bar', u'name': 'Foo'}
        ), query.edges().filter(name='Foo').filter(content='Bar').compile())

        self.assertEquals((
            u'g.E("name", name).has("content", content)',
            {u'content': 'Bar', u'name': 'Foo'}
        ), query.edges().filter_on_index('name', 'name', 'Foo').filter(content='Bar').compile())

        self.assertEquals((
            u'g.e(eid).has("name", name)',
            {'eid': 123, u'name': 'Foo'}
        ), query.edges().filter(eid=123).filter(name='Foo').compile())
        self.assertEquals((
            u'g.e(eid).has("name", name)',
            {'eid': 123, u'name': 'Foo'}
        ), query.edges().filter(eid=123, name='Foo').compile())


    def test_execute_raw_rexster(self):

        query = Query(self.session)
        query.execute_raw_groovy('g.V')
        query.execute_raw_groovy("g.V('element_type', 'Page').as('Page').in.as('Website').table(t);")
        query.execute_raw_groovy("t = new Table(); g.V('element_type', 'Page').as('Page').in.as('Website').table(t); return t;")

        assert False

    def test_build(self):

        # Tests if the

