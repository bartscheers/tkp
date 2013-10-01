"""
check database for consistency
"""
import logging
import time
import tkp.db

logger = logging.getLogger(__name__)
logdir = '/export/scratch2/bscheers/lofar/release1/performance/feb2013-sp6/napels/test/run_0/log'

# The three queries below were know to give a count > 0
# when the db got into an inconsistent state.
# Therefore, before every run we do a check on these numbers
query_id0 = """\
SELECT COUNT(*)
  FROM extractedsource
 WHERE id = 0
"""

query_image0 = """\
SELECT COUNT(*)
  FROM extractedsource
 WHERE image = 0
"""

query_zone0 = """\
SELECT COUNT(*)
  FROM extractedsource
 WHERE id = 0
   AND image = 0
   AND zone = 0
"""

def check():
    """
    Checks for any inconsistent values in tables.

    Returns False if any inconsistency is found, otherwise True.
    """
    for query in (query_id0, query_image0, query_zone0):
        if not isconsistent(query):
            return False
    return True

def isconsistent(query):
    """
    Counting rows should return 0, otherwise database is in an
    inconsistent state.

    If the database is consistent we return True, otherwise False.
    """
    try:
        logfile = open(logdir + '/' + isconsistent.__name__ + '.log', 'a')
        start = time.time()
        cursor = tkp.db.execute(query, commit=True)
        q_end = time.time() - start
        commit_end = time.time() - start
        logfile.write("-1," + str(q_end) + "," + str(commit_end) + "\n")
        result = cursor.fetchone()[0]
        if result == 0:
            return True
        else:
            logger.warning("Inconsistent database:\n %s returns %s" % (query, result))
    except Exception, e:
        logger.exception("No consistency check possible for database")
    return False
