class BaseQuery:
    def __init__(self, **kwargs):
        raise NotImplementedError

    def __str__(self):
        if hasattr(self, "query_text"):
            return self.query_text
        raise NotImplementedError(
            "attribute self.query_text must be implemented "
            f"in class {self.__class__.__name__}"
        )
