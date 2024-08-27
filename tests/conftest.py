"""
The conftest.py file serves as a means of providing fixtures for an entire directory.
Fixtures defined in a conftest.py can be used by any test in that package without
needing to import them (pytest will automatically discover them).

You can have multiple nested directories/packages containing your tests,
and each directory can have its own conftest.py with its own fixtures,
adding on to the ones provided by the conftest.py files in parent directories.

https://docs.pytest.org/en/7.2.x/reference/fixtures.html
"""

