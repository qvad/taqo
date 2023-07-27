from abc import ABC, abstractmethod


class PlanNodeAccessor(ABC):
    @staticmethod
    @abstractmethod
    def has_valid_cost(node):
        pass

    # ScanNode methods

    @staticmethod
    @abstractmethod
    def is_seq_scan(node):
        pass

    @staticmethod
    @abstractmethod
    def is_index_scan(node):
        pass

    @staticmethod
    @abstractmethod
    def is_index_only_scan(node):
        pass

    @staticmethod
    @abstractmethod
    def get_index_cond(node, with_label=False):
        pass

    @staticmethod
    @abstractmethod
    def may_have_table_fetch_by_rowid(node):
        pass

    @staticmethod
    @abstractmethod
    def get_remote_filter(node, with_label=False):
        pass

    # Table Fetch By Rowid
    @staticmethod
    @abstractmethod
    def get_remote_tfbr_filter(node, with_label=False):
        pass

    @staticmethod
    @abstractmethod
    def get_local_filter(node, with_label=False):
        pass

    @staticmethod
    @abstractmethod
    def get_rows_removed_by_recheck(node, with_label=False):
        pass
