"""
A collection of back end subroutines (mostly SQL queries).

In this module we collect together various routines
that don't fit into a more specific collection.
"""

import math
import logging
import itertools
import time

import tkp.db
from tkp.utility.coordinates import eq_to_cart
from tkp.utility.coordinates import alpha_inflate
from tkp.utility import substitute_inf


logger = logging.getLogger(__name__)
logdir = '/export/scratch2/bscheers/lofar/release1/performance/feb2013-sp6/napels/test/run_0/log'


lightcurve_query = """
SELECT im.taustart_ts
      ,im.tau_time
      ,ex.f_int
      ,ex.f_int_err
      ,ex.id
      ,im.band
      ,im.stokes
      ,bd.freq_central
  FROM extractedsource ex
      ,assocxtrsource ax
      ,image im
      ,frequencyband bd
 WHERE ax.runcat IN (SELECT runcat
                       FROM assocxtrsource
                      WHERE xtrsrc = %(xtrsrc)s
                    )
   AND ax.xtrsrc = ex.id
   AND ex.image = im.id
   AND bd.id = im.band
ORDER BY im.taustart_ts
"""

update_dataset_process_end_ts_query = """
UPDATE dataset
   SET process_end_ts = NOW()
 WHERE id = %(dataset_id)s
"""

def update_dataset_process_end_ts(dataset_id):
    """Update dataset start-of-processing timestamp.

    """
    args = {'dataset_id': dataset_id}
    logfile = open(logdir + '/' + update_dataset_process_end_ts.__name__ + '.log', 'a')
    start = time.time()
    tkp.db.execute(update_dataset_process_end_ts_query, args, commit=True)
    q_end = time.time() - start
    commit_end = time.time() - start
    logfile.write("-1," + str(q_end) + "," + str(commit_end) + "\n")
    return dataset_id


def insert_dataset(description):
    """Insert dataset with description as given by argument.

    DB function insertDataset() sets the necessary default values.
    """
    query = "SELECT insertDataset(%s)"
    arguments = (description,)
    logfile = open(logdir + '/' + insert_dataset.__name__ + '.log', 'a')
    start = time.time()
    cursor = tkp.db.execute(query, arguments, commit=True)
    q_end = time.time() - start
    commit_end = time.time() - start
    logfile.write("-1," + str(q_end) + "," + str(commit_end) + "\n")
    dataset_id = cursor.fetchone()[0]
    return dataset_id


def insert_monitor_positions(dataset_id, positions):
    """
    Add entries to the ``monitor`` table.

    Args:
      dataset_id (int): Positions will only be monitored when processing this
        dataset.
      positions (list of tuples): List of (RA, decl) tuples in decimal degrees.
    """

    monitor_entries = [(dataset_id, p[0], p[1]) for p in positions]

    insertion_query =  """\
INSERT INTO monitor
  (
  dataset
  ,ra
  ,decl
  )
VALUES {placeholder}
"""
    cols_per_row = 3
    placeholder_per_row = '('+ ','.join(['%s']*cols_per_row) +')'
    placeholder_full = ','.join([placeholder_per_row]*len(positions))
    logfile = open(logdir + '/' + insert_monitor_positions.__name__ + '.log', 'a')
    start = time.time()
    query = insertion_query.format(placeholder= placeholder_full)
    cursor = tkp.db.execute(
        query, tuple(itertools.chain.from_iterable(monitor_entries)),
        commit=True)
    q_end = time.time() - start
    commit_end = time.time() - start
    logfile.write("-1," + str(q_end) + "," + str(commit_end) + "\n")
    insert_num = cursor.rowcount
    logger.info("Inserted %d sources in monitor table for dataset %s" %
                    (insert_num, dataset_id))


def insert_image(dataset, freq_eff, freq_bw, taustart_ts, tau_time,
                 beam_smaj_pix, beam_smin_pix, beam_pa_rad, deltax, deltay, url,
                 centre_ra, centre_decl, xtr_radius, rms_qc,
                 rms_min, rms_max, detection_thresh, analysis_thresh
                 ):
    """Insert an image for a given dataset with the column values
    given in the argument list.

    Args:
     - restoring beam: beam_smaj_pix, beam_smin_pix are the semimajor and
       semiminor axes in pixel values; beam_pa_rad is the position angle
       in radians.
       They all will be converted to degrees, because that is unit used in
       the database.
     - centre_ra, centre_decl, xtr_radius:
       These define the region within ``xtr_radius`` degrees of the
       field centre, that will be used for source extraction.
       (This obviously implies a promised on behalf of the pipeline not to do
       anything else!)
       Note centre_ra, centre_decl, extracion_radius should all be in degrees.

    """
    query = """\
    SELECT insertImage(%(dataset)s
                      ,%(tau_time)s
                      ,%(freq_eff)s
                      ,%(freq_bw)s
                      ,%(taustart_ts)s
                      ,%(rb_smaj)s
                      ,%(rb_smin)s
                      ,%(rb_pa)s
                      ,%(deltax)s
                      ,%(deltay)s
                      ,%(url)s
                      ,%(centre_ra)s
                      ,%(centre_decl)s
                      ,%(xtr_radius)s
                      ,%(rms_qc)s
                      ,%(rms_min)s
                      ,%(rms_max)s
                      ,%(detection_thresh)s
                      ,%(analysis_thresh)s
                      )
    """
    arguments = {'dataset': dataset,
                 'tau_time': tau_time,
                 'freq_eff': freq_eff,
                 'freq_bw': freq_bw,
                 'taustart_ts': taustart_ts,
                 'rb_smaj': beam_smaj_pix * math.fabs(deltax),
                 'rb_smin': beam_smin_pix * math.fabs(deltay),
                 'rb_pa': 180 * beam_pa_rad / math.pi,
                 'deltax': deltax,
                 'deltay': deltay,
                 'url': url,
                 'centre_ra': centre_ra,
                 'centre_decl': centre_decl,
                 'xtr_radius': xtr_radius,
                 'rms_qc': rms_qc,
                 'rms_min': rms_min,
                 'rms_max': rms_max,
                 'detection_thresh': detection_thresh,
                 'analysis_thresh': analysis_thresh,
                 }
    logfile = open(logdir + '/' + insert_image.__name__ + '.log', 'a')
    start = time.time()
    cursor = tkp.db.execute(query, arguments, commit=True)
    q_end = time.time() - start
    commit_end = time.time() - start
    image_id = cursor.fetchone()[0]
    logfile.write(str(image_id) + "," + str(q_end) + "," + str(commit_end) + "\n")
    return image_id

def insert_extr_sources(image_id, results, extract_type,
                             ff_runcat_ids=None, ff_monitor_ids=None):

    delete_detections(image_id)
    insert_detections(image_id, results, extract_type, ff_runcat_ids=ff_runcat_ids, ff_monitor_ids=ff_monitor_ids)
    insert_extracted_sources_from_detections(image_id)
    delete_detections(image_id)

def delete_detections(image_id):
    """Delete temporary detections

    """
    query = """\
    DELETE FROM detection
    """
    logfile = open(logdir + '/' + delete_detections.__name__ + '.log', 'a')
    start = time.time()
    tkp.db.execute(query, commit=True)
    q_end = time.time() - start
    commit_end = time.time() - start
    logfile.write(str(image_id) + "," + str(q_end) + "," + str(commit_end) + "\n")

def insert_detections(image_id, results, extract_type,
                             ff_runcat_ids, ff_monitor_ids):
    """Insert all detections from sourcefinder straight into the database.


    The strict sequence from results (the sourcefinder detections) is given below.
    Note the units between sourcefinder and database.
    (0) ra [deg], (1) dec [deg],
    (2) ra_fit_err [deg], (3) decl_fit_err [deg],
    (4) peak_flux [Jy], (5) peak_flux_err [Jy],
    (6) int_flux [Jy], (7) int_flux_err [Jy],
    (8) significance detection level,
    (9) beam major width (arcsec), (10) - minor width (arcsec), (11) - parallactic angle [deg],
    (12) ew_sys_err [arcsec], (13) ns_sys_err [arcsec],
    (14) error_radius [arcsec]
    (15) gaussian fit (bool)

    We add
    (16) extract_type is either 0,1 or 2 of 'blind', 'ff_nd' or 'ff_ms' resp.
    (17) runcat, if extract_type is 'ff_nd' or 'ff_ms'
    """
    if not len(results):
        logger.info("No extract_type=%s sources added to extractedsource for"
                    " image %s" % (extract_type, image_id))
        return

    print "ff_runcat_ids =", ff_runcat_ids
    print "ff_monitor_ids =", ff_monitor_ids

    copyinto = "COPY %s RECORDS INTO detection FROM STDIN USING DELIMITERS ',', '\\n' NULL AS '';\n" % len(results)
    stdin = ""
    print "results =", results
    if extract_type == 'blind':
        for entry in results:
            # We add two fields which are '' => NULL
            stdin += ','.join(map(str, entry)) + ',0,\n'
    elif extract_type == 'ff_nd':
        for i, entry in enumerate(results):
            # We add one field (the last) which is '' => NULL
            stdin += ','.join(map(str, entry)) + ',1,' + str(ff_runcat_ids[i]) + '\n'
    elif extract_type == 'ff_ms':
        for i, entry in enumerate(results):
            # We add one field (the last) which is '' => NULL
            stdin += ','.join(map(str, entry)) + ',2,' + str(ff_monitor_ids[i]) + '\n'
    #print "before replace stdin = ",stdin
    #cpinp = stdin.replace('True', '1')
    #print "after replace stdin = ",cpinp
    #query = copyinto + cpinp

    query = copyinto + stdin.replace('True', '1')
    print "insert_detections; query:\n", query
    logfile = open(logdir + '/' + insert_detections.__name__ + '.log', 'a')
    start = time.time()
    cursor = tkp.db.execute(query, commit=True)
    q_end = time.time() - start
    commit_end = time.time() - start
    logfile.write(str(image_id) + "," + str(q_end) + "," + str(commit_end) + "\n")
    insert_num = cursor.rowcount
    logger.info("Inserted %d sources in detection for image %s" %
                    (insert_num, image_id))

def insert_extracted_sources_from_detections(image_id):
    """
    (0) ra [deg], (1) dec [deg],
    (2) ra_fit_err [deg], (3) decl_fit_err [deg],
    (4) peak_flux [Jy], (5) peak_flux_err [Jy],
    (6) int_flux [Jy], (7) int_flux_err [Jy],
    (8) significance detection level,
    (9) beam major width (arcsec), (10) - minor width (arcsec), (11) - parallactic angle [deg],
    (12) ew_sys_err [arcsec], (13) ns_sys_err [arcsec],
    (14) error_radius [arcsec]
    (15) gaussian fit (bool)

    """

    insertion_query = """\
INSERT INTO extractedsource
  (ra
  ,decl
  ,ra_fit_err
  ,decl_fit_err
  ,f_peak
  ,f_peak_err
  ,f_int
  ,f_int_err
  ,det_sigma
  ,semimajor
  ,semiminor
  ,pa
  ,ew_sys_err
  ,ns_sys_err
  ,error_radius
  ,fit_type
  ,ra_err
  ,decl_err
  ,uncertainty_ew
  ,uncertainty_ns
  ,image
  ,zone
  ,x
  ,y
  ,z
  ,racosdecl
  ,extract_type
  ,ff_runcat
  ,ff_monitor
  )
  SELECT ra
        ,decl
        ,ra_fit_err
        ,decl_fit_err
        ,f_peak
        ,f_peak_err
        ,f_int
        ,f_int_err
        ,det_sigma
        ,semimajor
        ,semiminor
        ,pa
        ,ew_sys_err
        ,ns_sys_err
        ,error_radius
        ,fit_type
        ,SQRT( ra_fit_err * ra_fit_err
             + alpha(ew_sys_err/3600, decl) * alpha(ew_sys_err/3600, decl)
             ) AS ra_err
        ,SQRT( decl_fit_err * decl_fit_err
             + ns_sys_err * ns_sys_err / 12960000)
             AS decl_err
        ,SQRT( ew_sys_err * ew_sys_err
             + error_radius * error_radius
             ) / 3600 AS uncertainty_ew
        ,SQRT( ns_sys_err * ns_sys_err
             + error_radius * error_radius
             ) / 3600 AS uncertainty_ns
        ,%(image_id)s AS image
        ,CAST(FLOOR(decl) AS INTEGER) AS zone
        ,COS(RADIANS(decl)) * COS(RADIANS(ra)) AS x
        ,COS(RADIANS(decl)) * SIN(RADIANS(ra)) AS y
        ,SIN(RADIANS(decl)) AS z
        ,ra * COS(RADIANS(decl)) AS racosdecl
        ,extract_type
        ,CASE WHEN extract_type = 1
              THEN runcat
              ELSE NULL
         END AS ff_runcat
        ,CASE WHEN extract_type = 2
              THEN runcat
              ELSE NULL
         END AS ff_monitor
    FROM detection

"""
    qry_params = {'image_id':image_id}
    logfile = open(logdir + '/' + insert_extracted_sources_from_detections.__name__ + '.log', 'a')
    start = time.time()
    cursor = tkp.db.execute(insertion_query, qry_params, commit=True)
    q_end = time.time() - start
    commit_end = time.time() - start
    logfile.write(str(image_id) + "," + str(q_end) + "," + str(commit_end) + "\n")
    insert_num = cursor.rowcount
    logger.info("Inserted %d sources in extractedsource for image %s" %
                (insert_num, image_id))
    #if insert_num == 0:
    #    logger.info("No forced-fit sources added to extractedsource for "
    #                "image %s" % (image_id,))
    #if extract_type == 'blind':
    #    logger.info("Inserted %d sources in extractedsource for image %s" %
    #                (insert_num, image_id))
    #elif extract_type == 'ff_nd':
    #    logger.info("Inserted %d forced-fit null detections in extractedsource"
    #                " for image %s" % (insert_num, image_id))
    #elif extract_type == 'ff_ms':
    #    logger.info("Inserted %d forced-fit for monitoring in extractedsource"
    #                " for image %s" % (insert_num, image_id))

def insert_extracted_sources(image_id, results, extract_type,
                             ff_runcat_ids=None, ff_monitor_ids=None):
    """
    Insert all detections from sourcefinder into the extractedsource table.

    Besides the source properties from sourcefinder, we calculate additional
    attributes that are increase performance in other tasks.

    The strict sequence from results (the sourcefinder detections) is given below.
    Note the units between sourcefinder and database.
    (0) ra [deg], (1) dec [deg],
    (2) ra_fit_err [deg], (3) decl_fit_err [deg],
    (4) peak_flux [Jy], (5) peak_flux_err [Jy],
    (6) int_flux [Jy], (7) int_flux_err [Jy],
    (8) significance detection level,
    (9) beam major width (arcsec), (10) - minor width (arcsec), (11) - parallactic angle [deg],
    (12) ew_sys_err [arcsec], (13) ns_sys_err [arcsec],
    (14) error_radius [arcsec]
    (15) gaussian fit (bool)

    ra_fit_err and decl_fit_err are the 1-sigma errors from the gaussian fit,
    in degrees. Note that for a source located towards the poles the ra_fit_err
    increases with absolute declination.
    error_radius is a pessimistic on-sky error estimate in arcsec.
    ew_sys_err and ns_sys_err represent the telescope dependent systematic errors
    and are in arcsec.
    An on-sky error (declination independent, and used in de ruiter calculations)
    is then:
    uncertainty_ew^2 = ew_sys_err^2 + error_radius^2
    uncertainty_ns^2 = ns_sys_err^2 + error_radius^2
    The units of uncertainty_ew and uncertainty_ns are in degrees.
    The error on RA is given by ra_err. For a source with an RA of ra and an error
    of ra_err, its RA lies in the range [ra-ra_err, ra+ra_err].
    ra_err^2 = ra_fit_err^2 + [alpha_inflate(ew_sys_err,decl)]^2
    decl_err^2 = decl_fit_err^2 + ns_sys_err^2.
    The units of ra_err and decl_err are in degrees.
    Here alpha_inflate() is the RA inflation function, it converts an
    angular on-sky distance to a ra distance at given declination.

    Input argument "extract" tells whether the source detections originate from:
    'blind': blind source extraction
    'ff_nd': from forced fits at null detection locations
    'ff_ms': from forced fits at monitoringlist positions

    Input argument ff_runcat is not empty in the case of forced fits from
    null detections. It contains the runningcatalog ids from which the
    source positions were derived for the forced fits. In that case the
    runcat ids will be inserted into the extractedsource table as well,
    to simplify further null-detection processing.
    For blind extractions this list is empty (None).

    For all extracted sources additional parameters are calculated,
    and appended to the sourcefinder data. Appended and converted are:

        - the image id to which the extracted sources belong to
        - the zone in which an extracted source falls is calculated, based
          on its declination. We adopt a zoneheight of 1 degree, so
          the floor of the declination represents the zone.
        - the positional errors are converted from degrees to arcsecs
        - the Cartesian coordinates of the source position
        - ra * cos(radians(decl)), this is very often being used in
          source-distance calculations
    """

    if not len(results):
        logger.info("No extract_type=%s sources added to extractedsource for"
                    " image %s" % (extract_type, image_id))
        return

    xtrsrc = []
    for i, src in enumerate(results):
        r = list(src)
        # Drop any fits with infinite flux errors
        if math.isinf(r[5]) or math.isinf(r[7]):
            logger.warn("Dropped source fit with infinite flux errors "
                        "at position %s %s" % (r[0], r[1]))
            continue
        # Use 360 degree rather than infinite uncertainty for
        # unconstrained positions.
        r[14] = substitute_inf(r[14], 360.0)
        r[15] = int(r[15])
        # ra_err: sqrt of quadratic sum of fitted and systematic errors.
        r.append(math.sqrt(r[2]**2 + alpha_inflate(r[12]/3600., r[1])**2))
        # decl_err: sqrt of quadratic sum of fitted and systematic errors.
        r.append(math.sqrt(r[3]**2 + (r[13]/3600.)**2))
        # uncertainty_ew: sqrt of quadratic sum of systematic error and error_radius
        # divided by 3600 because uncertainty in degrees and others in arcsec.
        r.append(math.sqrt(r[12]**2 + r[14]**2)/3600.)
        # uncertainty_ns: sqrt of quadratic sum of systematic error and error_radius
        # divided by 3600 because uncertainty in degrees and others in arcsec.
        r.append(math.sqrt(r[13]**2 + r[14]**2)/3600.)
        r.append(image_id) # id of the image
        r.append(int(math.floor(r[1]))) # zone
        r.extend(eq_to_cart(r[0], r[1])) # Cartesian x,y,z
        r.append(r[0] * math.cos(math.radians(r[1]))) # ra * cos(radians(decl))
        if extract_type == 'blind':
            r.append(0)
        elif extract_type == 'ff_nd':
            r.append(1)
        elif extract_type == 'ff_ms':
            r.append(2)
        else:
            raise ValueError("Not a valid extractedsource insert type: '%s'"
                             % extract_type)
        if ff_runcat_ids is not None:
            assert len(results)==len(ff_runcat_ids)
            r.append(ff_runcat_ids[i])
        else:
            r.append(None)

        if ff_monitor_ids is not None:
            assert len(results)==len(ff_monitor_ids)
            r.append(ff_monitor_ids[i])
        else:
            r.append(None)

        xtrsrc.append(r)
        print "xtrsrc:", xtrsrc

    insertion_query = """\
INSERT INTO extractedsource
  (ra
  ,decl
  ,ra_fit_err
  ,decl_fit_err
  ,f_peak
  ,f_peak_err
  ,f_int
  ,f_int_err
  ,det_sigma
  ,semimajor
  ,semiminor
  ,pa
  ,ew_sys_err
  ,ns_sys_err
  ,error_radius
  ,fit_type
  ,ra_err
  ,decl_err
  ,uncertainty_ew
  ,uncertainty_ns
  ,image
  ,zone
  ,x
  ,y
  ,z
  ,racosdecl
  ,extract_type
  ,ff_runcat
  ,ff_monitor
  )
VALUES {placeholder}
"""
    if xtrsrc:
        cols_per_row = len(xtrsrc[0])
        placeholder_per_row = '('+ ','.join(['%s']*cols_per_row) +')'

        placeholder_full = ','.join([placeholder_per_row]*len(xtrsrc))

        query = insertion_query.format(placeholder= placeholder_full)
        logfile = open(logdir + '/' + insert_extracted_sources.__name__ + '.log', 'a')
        start = time.time()
        cursor = tkp.db.execute(query, tuple(itertools.chain.from_iterable(xtrsrc)),
                                commit=True)
        q_end = time.time() - start
        commit_end = time.time() - start
        logfile.write(str(image_id) + "," + str(q_end) + "," + str(commit_end) + "\n")
        insert_num = cursor.rowcount
        #if insert_num == 0:
        #    logger.info("No forced-fit sources added to extractedsource for "
        #                "image %s" % (image_id,))
        if extract_type == 'blind':
            logger.info("Inserted %d sources in extractedsource for image %s" %
                        (insert_num, image_id))
        elif extract_type == 'ff_nd':
            logger.info("Inserted %d forced-fit null detections in extractedsource"
                        " for image %s" % (insert_num, image_id))
        elif extract_type == 'ff_ms':
            logger.info("Inserted %d forced-fit for monitoring in extractedsource"
                        " for image %s" % (insert_num, image_id))


def lightcurve(xtrsrcid):
    """
    Obtain a light curve for a specific extractedsource

    Args:

        xtrsrcid (int): the source identifier that corresponds to a point on
            the light curve. Note that the point does not have to be the start
            (first) point of the light curve.

    Returns:
        list: a list of tuples, each containing:
            - observation start time as a datetime.datetime object
            - integration time (float)
            - integrated flux (float)
            - integrated flux error (float)
            - database ID of this particular source
            - frequency band ID
            - stokes
    """
    args = {'xtrsrc': xtrsrcid}
    cursor = tkp.db.execute(lightcurve_query, args)
    return cursor.fetchall()
