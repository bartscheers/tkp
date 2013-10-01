"""
check image quality
"""
import logging
from collections import namedtuple
import tkp.db
import time

logger = logging.getLogger(__name__)
logdir = '/export/scratch2/bscheers/lofar/release1/performance/feb2013-sp6/napels/test/run_0/log'


# TODO: need to think of a way to sync this with tkp/db/tables/rejection.sql
RejectReason = namedtuple('RejectReason', 'id desc')

reason = {
    'rms': RejectReason(id=0, desc='RMS invalid'),
    'beam': RejectReason(id=1, desc='beam invalid'),
    'bright_source': RejectReason(id=2, desc='bright source near'),
    'tau_time': RejectReason(id=3, desc='tau_time invalid'),
}

query_reject = """\
INSERT INTO rejection
  (image
  ,rejectreason
  ,comment
  )
VALUES
  (%(imageid)s
  ,%(reason)s
  ,'%(comment)s'
  )
"""

query_unreject = """\
DELETE
  FROM rejection
 WHERE image=%(image)s
"""

query_isrejected = """\
SELECT rejectreason.description, rejection.comment
  FROM rejection, rejectreason
 WHERE rejection.rejectreason = rejectreason.id
   AND rejection.image = %(imageid)s
"""


def reject(imageid, reason, comment):
    """ Add a reject intro to the db for a given image
    :param imageid: The image ID of the image to reject
    :param reason: why is the image rejected, a defined in 'reason'
    :param comment: an optional comment with details about the reason
    """
    args = {'imageid': imageid, 'reason': reason, 'comment': comment}
    query = query_reject % args
    logfile = open(logdir + '/' + reject.__name__ + '.log', 'a')
    start = time.time()
    tkp.db.execute(query, commit=True)
    q_end = time.time() - start
    commit_end = time.time() - start
    logfile.write(str(imageid) + "," + str(q_end) + "," + str(commit_end) + "\n")


def unreject(imageid):
    """ Remove all rejection of a given imageid
    :param imageid: The image ID of the image to reject
    """
    query = query_unreject % {'image': imageid}
    logfile = open(logdir + '/' + unreject.__name__ + '.log', 'a')
    start = time.time()
    tkp.db.execute(query, commit=True)
    q_end = time.time() - start
    commit_end = time.time() - start
    logfile.write(str(imageid) + "," + str(q_end) + "," + str(commit_end) + "\n")


def isrejected(imageid):
    """ Find out if an image is rejected or not
    :param  imageid: The image ID of the image to reject
    :returns:  False if not rejected, a list of reason id's if rejected
    """
    query = query_isrejected % {'imageid': imageid}
    logfile = open(logdir + '/' + isrejected.__name__ + '.log', 'a')
    start = time.time()
    cursor = tkp.db.execute(query)
    q_end = time.time() - start
    commit_end = time.time() - start
    logfile.write(str(imageid) + "," + str(q_end) + "," + str(commit_end) + "\n")
    results = cursor.fetchall()
    if len(results) > 0:
        return ["%s: %s" % row for row in results]
    else:
        return False

