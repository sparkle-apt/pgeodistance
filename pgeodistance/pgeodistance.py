import contextlib
import os
import urllib.request
import warnings
from io import BytesIO
from typing import Any, Tuple, List
from zipfile import ZipFile

import pandas as pd

import haversine

STORAGE_DIR = os.environ.get(
    "PGEOCODE_DATA_DIR", os.path.join(os.path.expanduser("~"), "pgeocode_data")
)

# A list of download locations. If the first URL fails, following ones will
# be used.
DOWNLOAD_URL = [
    "https://download.geonames.org/export/zip/{country}.zip",
    "https://symerio.github.io/postal-codes-data/data/geonames/{country}.txt",
]


DATA_FIELDS = [
    "country_code",
    "postal_code",
    "place_name",
    "state_name",
    "state_code",
    "county_name",
    "county_code",
    "community_name",
    "community_code",
    "latitude",
    "longitude",
    "accuracy",
]

COUNTRIES_VALID = [
    "AD",
    "AR",
    "AS",
    "AT",
    "AU",
    "AX",
    "BD",
    "BE",
    "BG",
    "BM",
    "BR",
    "BY",
    "CA",
    "CH",
    "CO",
    "CR",
    "CZ",
    "DE",
    "DK",
    "DO",
    "DZ",
    "ES",
    "FI",
    "FO",
    "FR",
    "GB",
    "GF",
    "GG",
    "GL",
    "GP",
    "GT",
    "GU",
    "HR",
    "HU",
    "IE",
    "IM",
    "IN",
    "IS",
    "IT",
    "JE",
    "JP",
    "LI",
    "LK",
    "LT",
    "LU",
    "LV",
    "MC",
    "MD",
    "MH",
    "MK",
    "MP",
    "MQ",
    "MT",
    "MX",
    "MY",
    "NC",
    "NL",
    "NO",
    "NZ",
    "PH",
    "PK",
    "PL",
    "PM",
    "PR",
    "PT",
    "RE",
    "RO",
    "RU",
    "SE",
    "SI",
    "SJ",
    "SK",
    "SM",
    "TH",
    "TR",
    "UA",
    "US",
    "UY",
    "VA",
    "VI",
    "WF",
    "YT",
    "ZA",
]


@contextlib.contextmanager
def _open_extract_url(url: str, country: str) -> Any:
    """Download contents for a URL

    If the file has a .zip extension, open it and extract the country

    Returns the opened file object.
    """
    with urllib.request.urlopen(url) as res:
        with BytesIO(res.read()) as reader:
            if url.endswith(".zip"):
                with ZipFile(reader) as fh_zip:
                    with fh_zip.open(country.upper() + ".txt") as fh:
                        yield fh
            else:
                yield reader


@contextlib.contextmanager
def _open_extract_cycle_url(urls: List[str], country: str) -> Any:
    """Same as _open_extract_url but cycle through URLs until one works

    We start by opening the first URL in the list, and if fails
    move to the next, until one works or the end of list is reached.
    """
    if not isinstance(urls, list) or not len(urls):
        raise ValueError(f"urls={urls} must be a list with at least one URL")

    err_msg = f"Provided download URLs failed {{err}}: {urls}"
    for idx, val in enumerate(urls):
        try:
            with _open_extract_url(val, country) as fh:
                yield fh
            # Found a working URL, exit the loop.
            break
        except urllib.error.HTTPError as err:  # type: ignore
            if idx == len(urls) - 1:
                raise
            warnings.warn(
                f"Download from {val} failed with: {err}. "
                "Trying next URL in DOWNLOAD_URL list.",
                UserWarning,
            )
    else:
        raise ValueError(err_msg)

class GlobalGeoDistance:
    """
    * Support cross-country geo distance computation
    * Faster calculation
    """
    
    def __init__(self, force_download=False):
        self._unique_geo_data = {} # store unique geo data
        self._force_download = force_download
        
    @property
    def unique_geo_data(self):
        return self._unique_geo_data
    
    @property
    def force_download(self):
        return self._force_download
    
    def get_raw_geo_data_path(self, country):
        return os.path.join(STORAGE_DIR, country.upper() + ".txt")
    
    def get_unique_geo_data_path(self, country):
        return os.path.join(STORAGE_DIR, country.upper() + "-index.txt")
    
    def get_raw_geo_data(self, country):
        """
        Get raw geo data
        
        :param str country: country name
        
        :return: raw geo data
        :rtype: pandas.DataFrame
        """
        
        data_path = self.get_raw_geo_data_path(country)
        if os.path.exists(data_path) and not self.force_download:
            data = pd.read_csv(data_path, dtype={"postal_code": str})
        else:
            download_urls = [
                val.format(country=country) for val in DOWNLOAD_URL
            ]
            with _open_extract_cycle_url(download_urls, country) as fh:
                data = pd.read_csv(
                    fh,
                    sep="\t",
                    header=None,
                    names=DATA_FIELDS,
                    dtype={"postal_code": str},
                )
            os.makedirs(STORAGE_DIR, exist_ok=True)
            data.to_csv(data_path, index=None)
            
        return data
        
    def get_unique_geo_data(self, country):
        """
        Create or fetch a dataframe with unique postal codes
        
        :param str country: country name
        
        :return: raw geo data if success otherwise None
        :rtype: pandas.DataFrame
        """
        
        country = country.upper()
        
        if country in self._unique_geo_data:
            return self._unique_geo_data[country]
        
        if country not in COUNTRIES_VALID:
            print(
                (
                    "country={} is not a known country code. "
                    "See the README for a list of supported "
                    "countries"
                ).format(country)
            )
            return None
        
        if country == "AR":
            warnings.warn(
                "The Argentina data file contains 4-digit postal "
                "codes which were replaced with a new system "
                "in 1999."
            )
        
        raw_geo_data = self.get_raw_geo_data(country)
        
        unique_geo_data_path = self.get_unique_geo_data_path(country)

        if os.path.exists(unique_geo_data_path) and not self.force_download:
            unique_geo_data = pd.read_csv(
                unique_geo_data_path, dtype={"postal_code": str}
            )
        else:
            # group together places with the same postal code
            df_unique_cp_group = raw_geo_data.groupby("postal_code")
            unique_geo_data = df_unique_cp_group[["latitude", "longitude"]].mean()
            valid_keys = set(DATA_FIELDS).difference(
                ["place_name", "lattitude", "longitude", "postal_code"]
            )
            unique_geo_data["place_name"] = df_unique_cp_group["place_name"].apply(
                lambda x: ", ".join([str(el) for el in x])
            )
            for key in valid_keys:
                unique_geo_data[key] = df_unique_cp_group[key].first()
            unique_geo_data = unique_geo_data.reset_index()[DATA_FIELDS]
            unique_geo_data.to_csv(unique_geo_data_path, index=None)
            
        unique_geo_data = unique_geo_data.set_index('postal_code')
        self._unique_geo_data[country] = unique_geo_data
        
        return unique_geo_data
    
    def preprocess_postal_code(self, code, country):
        """
        Preprocess postal code
        
        For instance, take into account only first letters when applicable
        
        :param code: postal code, str or int
        :param str country: country name
        
        :return: postal code
        :rtype: str
        """
        
        if isinstance(code, int):
            code = str(code)
            
        if code == '':
            return code
        
        country = country.upper()
        
        code = code.upper()
        if country in ["GB", "IE", "CA"]:
            code = code.split()[0] # code != ''
            
        return code
    
    def get_geolocation(self, code, country):
        """
        Get locations information from postal code

        :param code: postal code, str or int
        :param str country: country name
        
        :return: (latitude, longitude) if success otherwise None
        :rtype: binary tuple of float
        """

        code = self.preprocess_postal_code(code, country)
        unique_geo_data = self.get_unique_geo_data(country)
        
        try:
            geolocation = tuple(
                unique_geo_data.loc[
                    code, ['latitude', 'longitude']
                ].values
            )
        except:
            # either unique_geo_data is None or code not supported
            raise
            return None
            
        return geolocation
    
    def query_postal_code(self, code_x, country_x, code_y, country_y):
        """
        Get distance (in km) between postal codes

        :param code_x: postal code, str or int
        :param str country_x: country name
        :param code_y: postal code, str or int
        :param str country_y: country name
        
        :return: calculated distance if success otherwise None
        :rtype: float
        """
        
        if code_x == code_y and country_x == country_y:
            return 0
        
        geolocation_x = self.get_geolocation(code_x, country_x)
        geolocation_y = self.get_geolocation(code_y, country_y)
        
        if geolocation_x is None or geolocation_y is None:
            return None
        
        dist = self.query_geolocation(geolocation_x, geolocation_y)
        
        return dist
        
    def query_geolocation(self, x, y):
        """
        :param tuple x: (latitude, longitude), tuple of float
        :param tuple y: (latitude, longitude), tuple of float
        
        :return: calculated distance
        :rtype: float
        """
        
        res = 0
        if x != y:
            res = haversine.haversine(x, y)
        return res