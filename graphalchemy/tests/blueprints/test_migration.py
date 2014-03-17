#! /usr/bin/env python
#-*- coding: utf-8 -*-

# ==============================================================================
#                                      IMPORTS
# ==============================================================================

from unittest import TestCase

# Services to test
from graphalchemy.blueprints.migration import MigrationGenerator

# Model
from graphalchemy.blueprints.schema import Node
from graphalchemy.blueprints.schema import Relationship
from graphalchemy.blueprints.schema import MetaData
from graphalchemy.blueprints.schema import Property
from graphalchemy.blueprints.schema import Adjacency

from graphalchemy.fixture.declarative import Page
from graphalchemy.fixture.declarative import Website
from graphalchemy.fixture.declarative import WebsiteHostsPage

# Types
from graphalchemy.blueprints.types import List
from graphalchemy.blueprints.types import String
from graphalchemy.blueprints.types import Boolean
from graphalchemy.blueprints.types import Url
from graphalchemy.blueprints.types import DateTime

# Auxiliary
from graphalchemy.ogm.mapper import Mapper


# ==============================================================================
#                                     TESTING
# ==============================================================================

class TestMigrationGenerator(TestCase):

    def setUp(self):
        self.maxDiff = None
        self.mapper = Mapper()
        self.metadata = MetaData()


    def test_node(self):

        website = Node('Website', self.metadata,
            Property('name', String(127), nullable=False, index='search', group='name', prefix=True),
            Property('domain', Url(), nullable=False, index=True, unique=True),
            Property('tags', List(), nullable=False, index=None),
        )
        page = Node('Page', self.metadata,
            Property('title', String(127), nullable=False),
            Property('url', Url(2801))
        )
        websiteHostsPage = Relationship('hosts', self.metadata,
            Property('since', DateTime, nullable=False, primaryKey=True),
            Property('accessible', Boolean()),
            group="rel"
        )

        websiteHostsPage_out = Adjacency(websiteHostsPage,
            direction=Relationship.OUT,
            unique=False,
            nullable=False
        )
        websiteHostsPage_in = Adjacency(page,
            direction=Relationship.IN,
            unique=True,
            nullable=False
        )

        self.mapper(WebsiteHostsPage, websiteHostsPage)
        self.mapper(Website, website, adjacencies={
            'hosts': websiteHostsPage_out
        })
        self.mapper(Page, page, adjacencies={
            'isHostedBy': websiteHostsPage_in
        })

        self.migration_generator = MigrationGenerator(self.metadata)

        self.migration_generator.run()

        self.assertEquals(self.migration_generator._queries, [
            '// Groups',
            'rel = TypeGroup.of(3, "rel");',
            'name = TypeGroup.of(2, "name");',
            '// Node Page',
            'url = graph.makeType().name("url").dataType(String.class).unique(Direction.OUT).makePropertyKey();',
            'title = graph.makeType().name("title").dataType(String.class).unique(Direction.OUT).makePropertyKey();',
            '// Node Website',
            'domain = graph.makeType().name("domain").dataType(String.class).indexed("standard", Vertex.class).unique(Direction.BOTH).makePropertyKey();',
            'Website_name = graph.makeType().group(name).name("Website_name").dataType(String.class).indexed("search", Vertex.class).unique(Direction.OUT).makePropertyKey();',
            'tags = graph.makeType().name("tags").dataType(Collection.class).makePropertyKey();',
            '// Relationship hosts',
            'accessible = graph.makeType().name("accessible").dataType(Integer.class).unique(Direction.OUT).makePropertyKey();',
            'since = graph.makeType().name("since").dataType(Integer.class).unique(Direction.OUT).makePropertyKey();',
            'hosts = graph.makeType().name("hosts").primaryKey(since).group(rel).directed().unique(IN).makeEdgeLabel();',
        ])

