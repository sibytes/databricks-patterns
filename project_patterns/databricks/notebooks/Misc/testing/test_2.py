# Databricks notebook source
# MAGIC %pip install unittest-xml-reporting

# COMMAND ----------

import xml.etree.ElementTree as ET
import unittest
import xmlrunner
import io


# COMMAND ----------

class Test2SampleTests1(unittest.TestCase):
  def tests_always_succeeds(self):
    self.assertTrue(True)
  
  def test_always_fails(self):
    self.assertTrue(False)

# COMMAND ----------

class Test2SampleTests2(unittest.TestCase):
  def tests_always_succeeds(self):
    self.assertTrue(True)
  
  def test_always_fails(self):
    self.assertTrue(False)

# COMMAND ----------

from inspect import getmro

def get_class_hierarchy(t):
  try:
    return getmro(t)
  except:
    return [object]

test_classes = {t for t in globals().values() if unittest.case.TestCase in get_class_hierarchy(t) and t != unittest.case.TestCase}
print(test_classes)

loader = unittest.TestLoader()
suite = unittest.TestSuite()
for test_class in test_classes:
  tests = loader.loadTestsFromTestCase(test_class)
  suite.addTests(tests)

out = io.BytesIO()
runner = xmlrunner.XMLTestRunner(out)
runner.run(suite)

# COMMAND ----------

out.seek(0)
test_results = out.read().decode('utf-8')

# COMMAND ----------

dbutils.notebook.exit(test_results)
