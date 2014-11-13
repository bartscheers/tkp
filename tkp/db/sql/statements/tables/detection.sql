CREATE TABLE detection
  (ra DOUBLE NOT NULL
  ,decl DOUBLE NOT NULL
  ,ra_fit_err DOUBLE NOT NULL
  ,decl_fit_err DOUBLE NOT NULL
  ,f_peak DOUBLE NULL
  ,f_peak_err DOUBLE NULL
  ,f_int DOUBLE NULL
  ,f_int_err DOUBLE NULL
  ,det_sigma DOUBLE NOT NULL
  ,semimajor DOUBLE NULL
  ,semiminor DOUBLE NULL
  ,pa DOUBLE NULL
  ,ew_sys_err DOUBLE NOT NULL
  ,ns_sys_err DOUBLE NOT NULL
  ,error_radius DOUBLE NOT NULL
  ,fit_type SMALLINT NOT NULL
  ,extract_type SMALLINT NOT NULL
  ,runcat INT 
  /*,ff_monitor INT*/
  )
;

