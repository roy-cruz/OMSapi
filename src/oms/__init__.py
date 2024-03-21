import os

from .get_oms_data import *
from .oms_utils import *
from .urls import API_URL, API_VERSION, API_AUDIENCE

API_CLIENT_ID = os.environ.get("API_CLIENT_ID")
API_CLIENT_SECRET = os.environ.get("API_CLIENT_SECRET")


class oms_fetch:
    """
    A class to fetch data from the OMS API.

    Attributes:
    - omsapi (OMSAPI): An instance of the OMSAPI class initialized with API details.

    Methods:
    - __init__(): Initializes the oms_fetch class with OMS API authentication.
    - get_oms_data(): Fetches data from a specified OMS API endpoint with various filters and arguments.
    """

    def __init__(self):
        """
        Initializes the oms_fetch instance by authenticating with the OMS API using provided credentials and settings.

        Sets up the OMS API client with specified API URL, version, and disables certificate verification for OIDC authentication.
        """
        self.omsapi = OMSAPI(
            api_url=API_URL, api_version=API_VERSION, cert_verify=False
        )
        self.omsapi.auth_oidc(API_CLIENT_ID, API_CLIENT_SECRET, audience=API_AUDIENCE)
        self.last_run_query = None
        self.last_ls_query = None

    def get_last_fetch(self):
        return self.last_query

    def get_oms_data(self, runs, attribs_level, attribs=[], extrafilters=[]):
        """
        Downloads run and lumisection level metadata for a given range of runs.

        Args:
            run_range (tuple): Start and end of range of runs to be loaded
            omsapi: API instance to load data from OMS
            attribs_level (string): Describes if the data to be downloaded are run or LS level features.
            attribs (list): Names of all of the attributes
            subdivs_steps (int): Number of (potential) runs after which the runs will be loaded sequentially instead of all at once.

        Returns:
            pd.DataFrame: Dataframe containing all of the requested features for all available runs.

        TO-DO:
            - [x] Add single run support
            - [] Add list of runs support
        """
        if isinstance(runs, int):
            runs = (runs, runs)

        if isinstance(runs, tuple):
            if attribs_level == "runs":
                limit_entries = 5000
                range_limit = limit_entries
            elif attribs_level == "lumisections":
                limit_entries = 100_000
                range_limit = 1000
            else:
                raise Exception("Error: Unrecognized OMS API endpoint.")

        if runs[1] - runs[0] > range_limit:
            run_range_list = self.subdivide_range(runs[0], runs[1], range_limit)
            oms_df = pd.DataFrame(columns=attribs)
            for runs in run_range_list:
                oms_json = self.get_oms_json(
                    attribs_level,
                    runs,
                    limit_entries=limit_entries,
                    attributes=attribs,
                    extrafilters=extrafilters,
                )

                oms_df = pd.concat([oms_df, makeDF(oms_json).convert_dtypes()])
                time.sleep(2)
        else:
            oms_json = self.get_oms_json(
                attribs_level,
                runs,
                limit_entries=limit_entries,
                attributes=attribs,
                extrafilters=extrafilters,
            )
            oms_df = makeDF(oms_json).convert_dtypes()

        oms_df.reset_index(inplace=True)

        if attribs_level == "runs":
            self.last_run_query = oms_df
        else:
            self.last_ls_query = oms_df

        return oms_df

    def get_oms_json(
        self,
        api_endpoint,
        runnb=None,
        extrafilters=[],
        extraargs={},
        sort=None,
        attributes=[],
        limit_entries=1000,
        as_df=False,
    ):
        """
        Fetches data from a specified OMS API endpoint applying various filters, sorting, and pagination options.

        Args:
        - api_endpoint (str): The target OMS API endpoint (e.g., 'runs' or 'lumisections').
        - runnb (int, tuple, list, None): Run number(s) to filter the data by. Can be an integer (single run), a tuple or list (range of runs), or None.
        - extrafilters (list): Additional filters to apply in the query. Each filter should be a dictionary specifying the attribute name, value, and comparison operator.
        - extraargs (dict): Extra key/value pairs to add to the query. Useful for custom queries or experimental features.
        - sort (str, None): Attribute name in the OMS data by which to sort the results. None for no sorting.
        - attributes (list): Specific attributes to return in the query result. An empty list returns all attributes.
        - limit_entries (int): The maximum number of entries to return in the response.
        - as_df (bool): If True, returns the response as a pandas DataFrame. Default is False, returning a raw JSON response.

        Returns:
        - dict or pandas.DataFrame: The OMS API query response in JSON format, or as a pandas DataFrame if as_df is True.

        Raises:
        - Exception: If the omsapi attribute is not an instance of OMSAPI, indicating a problem with API initialization.
        """
        filters = []

        # check omsapi argument
        if not isinstance(self.omsapi, OMSAPI):
            raise Exception(
                "ERROR in get_oms_data.py/get_oms_data:"
                + " first argument is of type "
                + str(type(self.omsapi))
                + " while and OMSAPI object is expected."
                + " You can use get_oms_api() to create this object."
            )
        # check runnb argument
        if runnb is None:
            pass  # special case: do not apply run number filter
        # elif isinstance(runnb, list):
        #     filters_lst = [0] * len(runnb)
        #     pass
        elif isinstance(runnb, int):
            filters.append(
                {attribute_name: "run_number", value: str(runnb), operator: "EQ"}
            )
        elif isinstance(runnb, tuple) or isinstance(runnb, list):
            filters.append(
                {attribute_name: "run_number", value: str(runnb[0]), operator: "GE"}
            )
            filters.append(
                {attribute_name: "run_number", value: str(runnb[1]), operator: "LE"}
            )
        else:
            print(
                "WARNING in get_oms_data.py/get_oms_data:"
                + " run number {} not recognized".format(runnb)
                + " (supposed to be an int, a tuple or list of 2 elements, or None)."
            )
        # check extrafilters argument
        expected_keys = sorted([attribute_name, value, operator])
        for extrafilter in extrafilters:
            keys = sorted(extrafilter.keys())
            if not keys == expected_keys:
                print(
                    "WARNING in get_oms_data.py/get_oms_data:"
                    + " filter {} contains unexpected keys".format(extrafilter)
                    + " (expecting only {}).".format(expected_keys)
                    + " The filter will be added but the query might fail..."
                )
            filters.append(extrafilter)

        q = self.omsapi.query(api_endpoint)
        if len(filters) > 0:
            q.filters(filters)
        if sort is not None:
            q.sort(sort)
        if len(attributes) is not None:
            q.attrs(attributes)
        for key, val in extraargs.items():
            q.custom(key, value=val)
        q.paginate(1, limit_entries)
        print(q.data_query())
        response = q.data()
        return response.json()

    def subdivide_range(oldest_run, newest_run, step=1000):
        """
        Subdivide a range from x to y into sub-ranges of size 'step'.

        Args:
            x (int): Start of the range.
            y (int): End of the range.
            step (int): The size of each sub-range.

        Returns:
            list of tuples: A list of tuples, each representing a sub-range.
        """
        ranges = []
        for start in range(oldest_run, newest_run, step):
            end = start + step
            if end > newest_run:
                ranges.append((start, newest_run))
            else:
                ranges.append((start, end - 1))

        return ranges
