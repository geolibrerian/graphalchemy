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
		from bulbs.rest import log
		log.setLevel(1)
		from graphalchemy.ogm.session import Session
		self.session = Session(client=client, metadata=metadata, logger=log)
		self.repository = Repository(self.session, page, Page, logger=log)


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


	def test_get(self):

		# @todo remove this dirty fixture
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


	def test_filter(self):

		results = self.repository.filter(title='Title').all()
		print results
		for result in results:
			self.session.delete(result)
		self.session.commit()
		print results



		assert False
