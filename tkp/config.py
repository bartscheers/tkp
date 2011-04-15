import ConfigParser
import os


"""

The config module stores the default values for various tunable parameters
in the TKP Python package.

The module also tries to read a '.tkp.cfg' file in the users home directory,
where parameters can be overriden (such as the database login details).

There are three sections in the configuration:

- databse

- source_association

- source_extraction

The parameters are stored in a dictionary, with each section as keyword.
Each value in itself is another dictionary, with (option, value) pairs.

The config module can be imported by the various subpackages of the
TKP package.  Since the '.tpk.cfg' file is only read on first import,
after which the variable HAS_READ is set to False, there are no multiple
reads of this file.

"""

_TO_DO = """\

To do:

- avoid the HAS_READ / NameError trick
  possibly by use of a singleton

- (optional) use class instead of dictionary to store options

"""



# Avoid eval
# This is very simple, and likely may be fooled by incorrect
# input
def double_list_from_string(text, contenttype=str):
    """Helper function to parse a double list from a string"""

    origtext = text[:]
    if contenttype not in (str, int, float, bool):
        raise ValueError("unknown or not allowed contenttype")
    text = origtext.strip()
    if text[0] != '[':
        raise ValueError("%s does not start with a list" % origtext)
    text = text[1:].strip()
    if text[0] != '[':
        raise ValueError("%s is not a valid double list" % origtext)
    text = text[1:].strip()
    elements = []
    while True:
        i = text.find(']')
        part = text[:i]
        elements.append(map(contenttype, part.split(',')))
        try:
            text = text[i+1:].strip()
        except IndexError:
            raise ValueError("%s is incorrect format" % origtext)
        if text[0] == ']':  # closing outer list
            break
        tmptext = text.lstrip(',').strip()
        if tmptext == text:
            raise ValueError("missing separating comma in %s" % origtext)
        text = tmptext.lstrip('[').strip()
        if tmptext == text:
            raise ValueError("missing opening bracket in %s" % origtext)
        
    return elements

    
def set_default_config():

    config = ConfigParser.SafeConfigParser()

    config.add_section('database')
    config.set('database', 'enabled', 'True')
    config.set('database', 'host', 'ldb001')
    config.set('database', 'name', 'tkp')
    config.set('database', 'user', 'tkp')
    config.set('database', 'password', 'tkp')
    config.set('database', 'port', '50000')
    
    config.add_section('source_association')
    config.set('source_association', 'deruiter_radius', '0.0010325')
    
    config.add_section('source_extraction')
    config.set('source_extraction', 'back_sizex', '32')
    config.set('source_extraction', 'back_sizey', '32')
    config.set('source_extraction', 'median_filter', '0')
    config.set('source_extraction', 'mf_threshold', '0')
    config.set('source_extraction', 'interpolate_order', '1')
    config.set('source_extraction', 'margin', '0')
    config.set('source_extraction', 'max_degradation', '0.2')
    config.set('source_extraction', 'fdr_alpha', '1e-2')
    config.set('source_extraction', 'structuring_element', '[[0,1,0], [1,1,1], [0,1,0]]')
    config.set('source_extraction', 'deblend', 'False')
    config.set('source_extraction', 'deblend_nthresh', '32')
    config.set('source_extraction', 'deblend_mincont', '0.005')
    config.set('source_extraction', 'detection_threshold', '10.0')
    config.set('source_extraction', 'analysis_threshold', '3.0')
    config.set('source_extraction', 'residuals', 'True')
    config.set('source_extraction', 'alpha_maj1', '2.5')
    config.set('source_extraction', 'alpha_min1', '0.5')
    config.set('source_extraction', 'alpha_maj2', '0.5')
    config.set('source_extraction', 'alpha_min2', '2.5')
    config.set('source_extraction', 'alpha_maj3', '1.5')
    config.set('source_extraction', 'alpha_min3', '1.5')
    config.set('source_extraction', 'clean_bias', '0.0')
    config.set('source_extraction', 'clean_bias_error', '0.0')
    config.set('source_extraction', 'frac_flux_cal_error', '0.0')
    config.set('source_extraction', 'eps_ra', '0.')
    config.set('source_extraction', 'eps_dec', '0.')

    from tkp.tests import __path__ as testpath
    config.add_section('test')
    config.set('test', 'datapath', os.path.join(testpath[0], "data"))
    return config


def read_config(default_config):
    """Attempt to read a user configuration file"""

    config = ConfigParser.SafeConfigParser()
    config.read(os.path.expanduser('~/.tkp.cfg'))

    # Check for unknown sections or options
    for section in config.sections():
        if not default_config.has_section(section):
            raise ConfigParser.Error("unknown section %s" % section)
        for option in config.options(section):
            if not default_config.has_option(section, option):
                raise ConfigParser.Error(
                    "unknown option %s in section %s" % (option, section))
    # Now overwrite default options
    for section in default_config.sections():
        if not config.has_section(section):
            config.add_section(section)
        for option in default_config.options(section):
            if not config.has_option(section, option):
                config.set(section, option, default_config.get(section, option))

    return config


def parse_config(config):
    """Parse the various config parameters into a dictionary, including type conversion"""

    # On to do list: create an inherited configparser that stores a type with the options,
    # and then does the parsing behind the scenes
    configuration = dict(database={}, source_association={}, source_extraction={})
    booleans = (('database', 'enabled'), ('source_extraction', 'deblend'),
                ('source_extraction', 'residuals'))
    integers = (('database', 'port'), ('source_extraction', 'back_sizex'),
                ('source_extraction', 'back_sizey'),
                ('source_extraction', 'median_filter'),
                ('source_extraction', 'interpolate_order'),
                ('source_extraction', 'deblend_nthresh'))
    floats = (('source_association', 'deruiter_radius'),
              ('source_extraction', 'mf_threshold'),
              ('source_extraction', 'margin'),
              ('source_extraction', 'max_degradation'),
              ('source_extraction', 'fdr_alpha'),
              ('source_extraction', 'deblend_mincont'),
              ('source_extraction', 'detection_threshold'),
              ('source_extraction', 'alpha_maj1'),
              ('source_extraction', 'alpha_min1'),
              ('source_extraction', 'alpha_maj2'),
              ('source_extraction', 'alpha_min2'),
              ('source_extraction', 'alpha_maj3'),
              ('source_extraction', 'alpha_min3'),
              ('source_extraction', 'clean_bias'),
              ('source_extraction', 'clean_bias_error'),
              ('source_extraction', 'frac_flux_cal_error'),
              ('source_extraction', 'eps_ra'),
              ('source_extraction', 'eps_dec'))
    configuration.update(dict([(section, dict(config.items(section)))
                               for section in config.sections()]))
    for section, option in booleans:
        try:
            configuration[section][option] = config.getboolean(section, option)
        except ValueError:
            raise ValueError(
        "incorrect type for option %s in section %s; must be boolean "
        "(True/False)" % (option, section))
    for section, option in integers:
        try:
            configuration[section][option] = config.getint(section, option)
        except ValueError:
            raise ValueError(
        "incorrect type for option %s in section %s; must be an integer" %
        (option, section))
    for section, option in floats:
        try:
            configuration[section][option] = config.getfloat(section, option)
        except ValueError:
            raise ValueError(
        "incorrect type for option %s in section %s; must be a real number" %
        (option, section))
    elements = double_list_from_string(config.get(
        'source_extraction', 'structuring_element'), contenttype=float)
    if (len(elements) != 3 or len(elements[0]) != 3 or len(elements[1]) != 3
        or len(elements[2]) != 3):
        raise ValueError(
            "incorrect type for structuring_element in section source_extraction")
    configuration['source_extraction']['structuring_element'] = elements
    
    return configuration


# This is a bit of a dirty trick; using some kind of singleton class
# with a class variable may be better, though I guess it's currently
# similar: a singleton module with module variable.
try:
    HAS_READ
except NameError, exc:
    config = read_config(set_default_config())
    config = parse_config(config)
    HAS_READ = True
else:
    pass