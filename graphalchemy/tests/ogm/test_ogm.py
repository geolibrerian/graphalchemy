#! /usr/bin/env python
#-*- coding: utf-8 -*-

# ==============================================================================
#                                      IMPORTS
# ==============================================================================

from unittest import TestCase

# Fixtures
from graphalchemy.fixture.declarative import Page
from graphalchemy.fixture.declarative import Website
from graphalchemy.fixture.declarative import WebsiteHostsPage
from graphalchemy.fixture.declarative import metadata

# Services

# ==============================================================================
#                                     TESTING
# ==============================================================================

class RepositoryTestCase(TestCase):

    def setUp(self):
        from bulbs.titan import TitanClient
        client = TitanClient(db_name="graph")
        from graphalchemy.ogm.session import OGM
        self.ogm = OGM(client=client, metadata=metadata)


    def test_persist_relation(self):

        website1 = Website(
            name='AllRecipes',
            domain='allrecipes.com',
            description='The biggest recipe website !',
            content='100K+ yummy recipes.'
        )
        page1 = Page(
            title='Apple pie',
            url='http://allrecipes.com/recipe/123',
        )
        page2 = Page(
            title='Shepherds pie',
            url='http://allrecipes.com/recipe/345',
        )
        whp1 = WebsiteHostsPage()
        whp2 = WebsiteHostsPage()
        website1.hosts[whp1] = page1
        website1.hosts[whp2] = page2

        self.ogm.add(page1)
        self.ogm.add(page2)
        self.ogm.add(website1)
        self.ogm.add(whp1)
        self.ogm.add(whp2)
        self.ogm.commit()

        self.assertTrue(website1.id is not None)
        self.assertTrue(page1.id is not None)
        self.assertTrue(page2.id is not None)
        self.assertTrue(whp1.id is not None)
        self.assertTrue(whp2.id is not None)

        # query = self.ogm.query("g.v(%i).outE" % (website1.id, ), {})
        query = self.ogm.query("g.v(eid).out('hosts')", {'eid': website1.id})
        print query._results
        assert False














