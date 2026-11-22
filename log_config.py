import logging


class ConciseFormatter(logging.Formatter):
    """Keep the exception type and cause without a multiline stack trace."""

    def formatException(self, exc_info):
        error = exc_info[1]
        parts = []
        seen = set()
        while error is not None and id(error) not in seen:
            seen.add(id(error))
            detail = " ".join(str(error).split())
            parts.append(f"{type(error).__name__}: {detail}" if detail else type(error).__name__)
            error = error.__cause__ or (None if error.__suppress_context__ else error.__context__)
        return "Причина: " + " → ".join(parts)


def configure_logging():
    handler = logging.StreamHandler()
    handler.setFormatter(ConciseFormatter("%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
