#! /usr/bin/env python
#-*- coding: utf-8 -*-

# ==============================================================================
#                                      IMPORTS
# ==============================================================================

from unittest import TestCase

# Services
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
        from graphalchemy.ogm.session import Session
        self.session = Session(client=client, metadata=metadata)
        self.repository = Repository(self.session, page, Page)


    def test_create(self):
        # With create method
        obj = self.repository.create(title='Title', url="http://allrecipes.com/page/1")
        self.assertIsInstance(obj, Page)
        self.assertEquals(obj.title, 'Title')
        self.assertEquals(obj.url, "http://allrecipes.com/page/1")

        # With direct call
        obj = self.repository(title='Title', url="http://allrecipes.com/page/1")
        self.assertIsInstance(obj, Page)
        self.assertEquals(obj.title, 'Title')
        self.assertEquals(obj.url, "http://allrecipes.com/page/1")


    def test_all(self):

        website_obj = Page(title='Title', url="http://allrecipes.com/page/1")

        self.session.add(website_obj)
        self.session.commit()

        # If the session is not cleared, the entity stays in the entity map and
        # returns the same object.
        obj = self.repository.get(website_obj.id)
        self.assertEquals(obj.id, website_obj.id)
        self.assertEquals(obj.title, 'Title')
        self.assertEquals(obj.url, "http://allrecipes.com/page/1")
        self.assertIsInstance(obj, Page)
        self.assertIs(obj, website_obj)

        # If the session is cleared, the entity disappears from the entity map and
        # a new object is returned.
        self.session.clear()
        obj = self.repository.get(website_obj.id)
        self.assertEquals(obj.id, website_obj.id)
        self.assertEquals(obj.title, 'Title')
        self.assertEquals(obj.url, "http://allrecipes.com/page/1")
        self.assertIsInstance(obj, Page)
        self.assertIsNot(obj, website_obj)

        # Filter
        results = self.repository.filter(title='Title').all()
        self.assertTrue(len(results) > 0)

        # Delete
        for result in results:
            self.session.delete(result)
        self.session.commit()
        results = self.repository.filter(title='Title').all()
        self.assertEquals(len(results), 0)


    def test_multiple(self):

        # Truncate
        self.repository.truncate()
        results = self.repository.filter(title='TitleUnique').all()
        self.assertEquals(len(results), 0)

        # Multiple additions of same object
        website_obj = Page(title='TitleUnique', url="http://allrecipes.com/page/1")
        self.session.add(website_obj)
        self.session.add(website_obj)
        self.session.commit()
        self.session.add(website_obj)
        self.session.commit()

        # Filter
        results = self.repository.filter(title='TitleUnique').all()
        self.assertEquals(len(results), 1)


