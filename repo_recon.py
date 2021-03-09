# coding=utf-8
#
# Read repo reconciliation file from Bloomberg AIM, enrich it with
# collateral id and quantity from datastore, then create the new
# reconciliation file.
# 

from toolz.functoolz import compose
from functools import partial
import logging
logger = logging.getLogger(__name__)



"""
1. load Bloomberg recon file

2. map it to new recon file

3. save it

4. upload

5. notify

6. move
"""



def enrichReconFile(aimFile):
	"""
	[String] Bloomberg AIM reconciliation file
		=> [String] Enriched reconciliation file

	Side effect: produce a new reconciliation file in the same directory
	as the AIM reconciliation file.
	"""
	data = readAIMFile(aimFile)

	"""
	filter out closed or canceled positions;

	for each remains, lookup datastore to find below:

	collateral id,
	collateral quantity,
	repo currency (confirm?)

	if not found, leave log and fill in empty

	write enriched information back
	"""
	return ''



