#! /usr/bin/env python
#-*- coding: utf-8 -*-

# ==============================================================================
#                                      IMPORTS
# ==============================================================================

from unittest import TestCase

# Services to test
from graphalchemy.blueprints.model_visualization.generator import GraphvizVisualizationGenerator

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

import re

# ==============================================================================
#                                     TESTING
# ==============================================================================

class TestGraphvizVisualizationGenerator(TestCase):

    def setUp(self):
        self.mapper = Mapper()
        self.metadata = MetaData()

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
            Property('accessible', Boolean())
        )

        websiteHostsPage_out = Adjacency(node=website,
            relationship=websiteHostsPage,
            direction=Relationship.OUT,
            unique=False,
            nullable=False
        )
        websiteHostsPage_in = Adjacency(node=page,
            relationship=websiteHostsPage,
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

        self.title_test_value = 'Test - graph title'
        self.visualization_generator = GraphvizVisualizationGenerator(self.metadata)

        self.visualization_generator \
            .set_graph_title(self.title_test_value) \
            .run()

    def test_title(self):
        self.assertEquals(self.visualization_generator.graph_title,
                          self.title_test_value)

    def test_nodes(self):
        # assertIn
        node_test_values = ['node_Page [ label =  "[Page] \\n\\n\\ - url\\n\\\n- title",\n $$  ];',
                            'node_Website [ label =  "[Website] \\n\\n\\ - domain\\n\\\n- Website_name\\n\\\n- tags",\n $$  ];']
        for node in self.visualization_generator._nodes.values():
            node = re.sub('color = ".*"', "$$", node)
            self.assertIn(node, node_test_values)

    def test_relationships(self):
        self.assertEquals(self.visualization_generator._relationships.values(),
                          ['"node_Website" -> "node_Page" [ label = "hosts \\n\\ - accessible\\n\\\n- since " taillabel="N\t" headlabel="\t1" ];']
                          )



