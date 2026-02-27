"""
Unit tests for lambda/model/BusinessTermHierarchyIndex.py
"""
import pytest
from unittest.mock import MagicMock

from model.BusinessTermHierarchyIndex import BusinessTermHierarchyIndex


@pytest.mark.unit
class TestBusinessTermHierarchyIndex:
    """Tests for BusinessTermHierarchyIndex class"""

    @pytest.fixture
    def mock_glossary_cache(self):
        """Provides a mock SMUSGlossaryCache"""
        cache = MagicMock()
        cache.is_term_present.return_value = True
        cache.get_smus_term_id.side_effect = lambda name: f"id-{name}"
        return cache

    def test_init_creates_empty_index(self, mock_glossary_cache):
        """Test initialization creates empty index"""
        index = BusinessTermHierarchyIndex(mock_glossary_cache)
        
        assert list(index.get_indexed_term_names()) == []

    def test_index_adds_parent_child_relationship(self, mock_glossary_cache):
        """Test indexing adds parent-child relationship"""
        index = BusinessTermHierarchyIndex(mock_glossary_cache)
        
        index.index("child_term", "parent_term")
        
        indexed_names = list(index.get_indexed_term_names())
        assert "child_term" in indexed_names
        assert "parent_term" in indexed_names

    def test_index_creates_isA_relationship_for_child(self, mock_glossary_cache):
        """Test indexing creates isA relationship for child term"""
        index = BusinessTermHierarchyIndex(mock_glossary_cache)
        
        index.index("child_term", "parent_term")
        
        child_relations = index.get_term_relations("child_term")
        assert "isA" in child_relations
        assert "id-parent_term" in child_relations["isA"]

    def test_index_creates_classifies_relationship_for_parent(self, mock_glossary_cache):
        """Test indexing creates classifies relationship for parent term"""
        index = BusinessTermHierarchyIndex(mock_glossary_cache)
        
        index.index("child_term", "parent_term")
        
        parent_relations = index.get_term_relations("parent_term")
        assert "classifies" in parent_relations
        assert "id-child_term" in parent_relations["classifies"]

    def test_index_handles_multiple_parents(self, mock_glossary_cache):
        """Test indexing handles multiple parents for same child"""
        index = BusinessTermHierarchyIndex(mock_glossary_cache)
        
        index.index("child_term", "parent1")
        index.index("child_term", "parent2")
        
        child_relations = index.get_term_relations("child_term")
        assert len(child_relations["isA"]) == 2
        assert "id-parent1" in child_relations["isA"]
        assert "id-parent2" in child_relations["isA"]

    def test_index_handles_multiple_children(self, mock_glossary_cache):
        """Test indexing handles multiple children for same parent"""
        index = BusinessTermHierarchyIndex(mock_glossary_cache)
        
        index.index("child1", "parent_term")
        index.index("child2", "parent_term")
        
        parent_relations = index.get_term_relations("parent_term")
        assert len(parent_relations["classifies"]) == 2
        assert "id-child1" in parent_relations["classifies"]
        assert "id-child2" in parent_relations["classifies"]

    def test_index_skips_when_child_not_in_cache(self, mock_glossary_cache):
        """Test indexing skips when child term not in cache"""
        mock_glossary_cache.is_term_present.side_effect = lambda name: name != "missing_child"
        index = BusinessTermHierarchyIndex(mock_glossary_cache)
        
        index.index("missing_child", "parent_term")
        
        assert list(index.get_indexed_term_names()) == []

    def test_index_skips_when_parent_not_in_cache(self, mock_glossary_cache):
        """Test indexing skips when parent term not in cache"""
        mock_glossary_cache.is_term_present.side_effect = lambda name: name != "missing_parent"
        index = BusinessTermHierarchyIndex(mock_glossary_cache)
        
        index.index("child_term", "missing_parent")
        
        assert list(index.get_indexed_term_names()) == []

    def test_get_term_relations_returns_empty_dict_for_unknown_term(self, mock_glossary_cache):
        """Test get_term_relations returns empty dict for unknown term"""
        index = BusinessTermHierarchyIndex(mock_glossary_cache)
        
        relations = index.get_term_relations("unknown_term")
        
        assert relations == {}

    def test_get_term_relations_limits_isA_to_10(self, mock_glossary_cache):
        """Test get_term_relations limits isA relationships to 10"""
        index = BusinessTermHierarchyIndex(mock_glossary_cache)
        
        # Add 15 parents
        for i in range(15):
            index.index("child_term", f"parent{i}")
        
        child_relations = index.get_term_relations("child_term")
        assert len(child_relations["isA"]) == 10

    def test_get_term_relations_limits_classifies_to_10(self, mock_glossary_cache):
        """Test get_term_relations limits classifies relationships to 10"""
        index = BusinessTermHierarchyIndex(mock_glossary_cache)
        
        # Add 15 children
        for i in range(15):
            index.index(f"child{i}", "parent_term")
        
        parent_relations = index.get_term_relations("parent_term")
        assert len(parent_relations["classifies"]) == 10

    def test_get_indexed_term_names_returns_all_terms(self, mock_glossary_cache):
        """Test get_indexed_term_names returns all indexed terms"""
        index = BusinessTermHierarchyIndex(mock_glossary_cache)
        
        index.index("child1", "parent1")
        index.index("child2", "parent2")
        
        names = list(index.get_indexed_term_names())
        assert len(names) == 4
        assert "child1" in names
        assert "child2" in names
        assert "parent1" in names
        assert "parent2" in names

    def test_index_entry_only_includes_non_empty_relations(self, mock_glossary_cache):
        """Test IndexEntry only includes non-empty relations"""
        index = BusinessTermHierarchyIndex(mock_glossary_cache)
        
        # Create a term that only has isA relationships
        index.index("child_term", "parent_term")
        
        # Parent should only have classifies, not isA
        parent_relations = index.get_term_relations("parent_term")
        assert "classifies" in parent_relations
        assert "isA" not in parent_relations
