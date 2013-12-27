#! /usr/bin/env python
#-*- coding: utf-8 -*-

# ==============================================================================
#                                      IMPORTS
# ==============================================================================

from unittest import TestCase

# Services to test
from graphalchemy.blueprints.validation import Validator

# Model
from graphalchemy.ogm.mapper import Mapper
from graphalchemy.blueprints.schema import MetaData
from graphalchemy.blueprints.schema import Relationship
from graphalchemy.blueprints.schema import Node
from graphalchemy.blueprints.schema import Adjacency


# ==============================================================================
#                                     TESTING
# ==============================================================================

class RelationshipTestCase(TestCase):

    def test_apply(self):

        metadata = MetaData()
        mapper = Mapper()

        class Page(object):
            pass
        class Website(object):
            pass
        class WebsiteHostsPage(object):
            pass
        website = Node('Website', metadata)
        page = Node('Page', metadata)
        websiteHostsPage = Relationship('hosts', metadata)
        websiteHostsPage_adj = Adjacency(website, websiteHostsPage, page, unique=False, nullable=True)
        mapper(WebsiteHostsPage, websiteHostsPage)
        mapper(Page, page, adjacencies={'isHostedBy': websiteHostsPage_adj})
        mapper(Website, website, adjacencies={'hosts': websiteHostsPage_adj})

        # Fixtures
        website1 = Website()
        website2 = Website()
        page1 = Page()
        page2 = Page()
        page3 = Page()
        whp1 = WebsiteHostsPage()

        # Relationship initialization
        # ---------------------------------
        self.assertFalse(website1.hosts is website2.hosts)
        self.assertEquals(website1.hosts, {})
        self.assertEquals(website1.hosts.keys(), [])
        self.assertEquals(website1.hosts.values(), [])

        # Relationship addition : assignment
        # ---------------------------------
        website1.hosts[whp1] = page1

        # Direct
        keys = website1.hosts.keys()
        values = website1.hosts.values()
        self.assertEquals(len(keys), 1)
        self.assertEquals(len(values), 1)
        self.assertIn(whp1, keys)
        self.assertIn(page1, values)

        # Reverse
        keys = page1.isHostedBy.keys()
        values = page1.isHostedBy.values()
        self.assertEquals(len(keys), 1)
        self.assertEquals(len(values), 1)
        self.assertIn(whp1, keys)
        self.assertIn(website1, values)

        # Relation
        self.assertIs(whp1.inV, page1)
        self.assertIs(whp1.outV, website1)

        # Relationship addition : None assignment
        # ---------------------------------
        website1.hosts[None] = page2
        keys = website1.hosts.keys()
        values = website1.hosts.values()
        self.assertEquals(len(keys), 2)
        self.assertIn(whp1, keys)
        self.assertNotIn(None, keys)
        # self.assertIn(whp2, keys)
        self.assertEquals(len(values), 2)
        self.assertIn(page1, values)
        self.assertIn(page2, values)
        keys = page2.isHostedBy.keys()
        values = page2.isHostedBy.values()
        self.assertEquals(len(keys), 1)
        self.assertEquals(len(values), 1)
        # self.assertIn(whp1, keys)
        self.assertIn(website1, values)

        # Relationship addition : append assignment
        # ---------------------------------
        website1.hosts.append(page3)
        keys = website1.hosts.keys()
        values = website1.hosts.values()
        self.assertEquals(len(keys), 3)
        self.assertNotIn(None, keys)
        self.assertEquals(len(values), 3)
        self.assertIn(page1, values)
        self.assertIn(page2, values)
        self.assertIn(page3, values)
        keys = page3.isHostedBy.keys()
        values = page3.isHostedBy.values()
        self.assertEquals(len(keys), 1)
        self.assertEquals(len(values), 1)
        # self.assertIn(whp1, keys)
        self.assertIn(website1, values)


