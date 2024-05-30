from __future__ import annotations

import pytest

from gaarf.io.writers import null_writer


class TestNullWriter:
  def test_init_null_writer_raises_value_error(self):
    with pytest.raises(ValueError):
      null_writer.NullWriter('non-existing-option')
