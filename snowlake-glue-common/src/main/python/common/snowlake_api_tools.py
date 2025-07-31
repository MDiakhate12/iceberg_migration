# -*- coding: utf-8 -*-
# %%
import requests
from http import HTTPStatus
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import logging
import time
import boto3
import secrets
import concurrent.futures
from abc import ABC, abstractmethod
import jwt
import json
# %%

logger = logging.getLogger(__name__)


class PaginationStrategy(ABC):
    """
    Classe abstraite pour définir une stratégie de pagination.
    Toute classe dérivée doit implémenter ces méthodes.
    """

    @abstractmethod
    def init_page(self):
        """Initialise les informations de pagination (offset, cursor, page_number, etc.)."""
        pass

    @abstractmethod
    def get_next_params(self, page_info):
        """Retourne les paramètres à utiliser pour la requête en fonction de la pagination."""
        pass

    @abstractmethod
    def update_page(self, page_info):
        """Met à jour l'information de pagination pour la requête suivante."""
        pass

    @abstractmethod
    def is_last_page(self, page_data_group):
        """Vérifie si on a atteint la dernière page de la pagination."""
        pass


class OffsetPagination(PaginationStrategy):
    def __init__(self, page_size, limit_param="$top", offset_param="$skip", initial_offset=0):
        """
        :param page_size: Nombre d'éléments par page
        :param limit_param: Nom du paramètre pour la taille de la page (ex: '$top', 'limit', 'count')
        :param offset_param: Nom du paramètre pour le décalage (ex: '$skip', 'offset', 'start')
        """
        self.page_size = page_size
        self.limit_param = limit_param
        self.offset_param = offset_param
        self.initial_offset = initial_offset

    def init_page(self, initial_offset=None):
        self.initial_offset = initial_offset or 0
        return self.initial_offset  # Offset initial

    def get_next_params(self, offset):
        """Génère les paramètres de requête avec des noms de clés personnalisables."""
        return {
            self.limit_param: self.page_size,
            self.offset_param: offset
        }

    def update_page(self, offset):
        """Met à jour l'offset pour récupérer la page suivante."""
        return offset + self.page_size

    def is_last_page(self, page_data_group, data_key):
        """Vérifie si l'une des pages récupérées est vide, indiquant la fin de la pagination."""

        pages = [
            page.get(data_key, []) if type(page) is dict
            else page
            for page in page_data_group
        ]

        return any(len(page) == 0 for page in pages)


class ApiConnector:

    def get(self, url, method="GET", max_retries=10, backoff_factor=3, **kwargs):
        """Fetch data from API URL for a specific data source"""

        # Configurer une stratégie de retries
        retry_strategy = Retry(
            total=max_retries,  # Nombre total de retries
            status_forcelist=[500, 502, 503, 504, 429],  # Codes d'erreur pour lesquels réessayer
            method_whitelist=["GET", "POST"],  # Méthodes autorisées pour les retries
            backoff_factor=backoff_factor  # Un petit backoff exponentiel
        )

        http_adapter = HTTPAdapter(max_retries=retry_strategy)
        session = requests.Session()
        session.mount('http://', http_adapter)
        session.mount('https://', http_adapter)

        try:
            if "params" in kwargs:

                params = kwargs.pop("params")

                url_params = "&".join(f"{k}={v}" for k, v in params.items())

                url = f"{url}?{url_params}"

            data_key = kwargs.pop("data_key", None)
            extract_key = kwargs.pop("extract_key", None)

            logger.info(f"Start fetching data from API for {url=}")

            start_time = time.time()

            response = session.request(method=method, url=url, **kwargs)

            duration = time.time() - start_time

            if response.status_code == HTTPStatus.OK:
                data = response.json()

                data_length = len(data[data_key]) if data_key and data_key in data else len(data)
                logger.info(f"Successfully fetched data from API for {url=} len(data) = {data_length} with status code: {response.status_code} in {duration:.2f} seconds")

                show_metadata_infos = kwargs.pop("show_metadata_infos", None)

                if data_key and show_metadata_infos:
                    metadata_infos = {k: v for k, v in data.items() if k != data_key}
                    logger.info(f"Batch metadata {json.dumps(metadata_infos, indent=2)}")

                if extract_key:
                    data = data[extract_key]

                return data
            else:
                logger.error(f"API request failed for {url=} with status code: {response.status_code}, {response.text}, {response.reason}")
                return []

        except requests.exceptions.Timeout as e:
            logger.error(f"API request timed out for data source {url=} - {e}")
            return []

        except Exception as e:
            logger.error(f"Error when trying to request API for {url=}: {e}")
            return []

    def fetch_pages(self, url, pagination_strategy, max_workers=10, data_key="value", params={}, source_name="", **kwargs):
        """
        Fonction générique pour récupérer des pages de données avec différentes stratégies de pagination.

        :param datasource: Un objet contenant l'URL et les paramètres de requête
        :param pagination_strategy: Une instance d'une classe de stratégie de pagination
        :param max_workers: Nombre de pages à récupérer en parallèle
        :param data_key: Clé contenant les données dans la réponse
        :return: Liste des données récupérées
        """

        data = []
        next_page_info = pagination_strategy.init_page()  # Initialisation de la pagination

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                futures = []

                # Lancer des requêtes en parallèle pour le batch actuel
                for _ in range(max_workers):
                    if next_page_info is None:
                        break  # Stop si on atteint la fin

                    params = {
                        **params,
                        **pagination_strategy.get_next_params(next_page_info)
                    }
                    futures.append(executor.submit(self.get, url=url, params=params, data_key=data_key, **kwargs))
                    next_page_info = pagination_strategy.update_page(next_page_info)

                if not futures:
                    break  # Stop si aucune requête n'a été lancée

                page_data_group = [future.result() for future in concurrent.futures.as_completed(futures)]

                for page_data in page_data_group:
                    values = page_data.get(data_key, []) if data_key else page_data
                    data.extend(values)

                if pagination_strategy.is_last_page(page_data_group, data_key):
                    break  # Arrêter si la pagination atteint la fin

        logger.info(f'{source_name} - Fetched a total of {len(data)} records')
        return data

    def fetch_pages_lazy(self, url, pagination_strategy: PaginationStrategy, max_workers=10, data_key=None, params={}, source_name="", **kwargs):
        """
        Version paresseuse de fetch_pages qui utilise yield pour retourner chaque batch au fur et à mesure.

        :param url: URL de l'API
        :param pagination_strategy: Stratégie de pagination
        :param max_workers: Nombre de pages à récupérer en parallèle
        :param data_key: Clé contenant les données dans la réponse
        :param params: Paramètres additionnels pour la requête
        :param source_name: Nom de la source (pour logging)
        :yield: Liste des données récupérées par batch
        """

        next_page_info = pagination_strategy.init_page()  # Initialisation de la pagination
        has_data = True

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            while has_data:
                futures = []

                # Lancer des requêtes en parallèle pour le batch actuel
                for _ in range(max_workers):
                    if next_page_info is None:
                        break  # Stop si on atteint la fin

                    page_params = {
                        **params,
                        **pagination_strategy.get_next_params(next_page_info)
                    }
                    futures.append(executor.submit(self.get, url=url, params=page_params, data_key=data_key, **kwargs))
                    next_page_info = pagination_strategy.update_page(next_page_info)

                if not futures:
                    break  # Stop si aucune requête n'a été lancée

                # Récupération des résultats du batch
                page_data_group = [future.result() for future in concurrent.futures.as_completed(futures)]
                batch_data = []

                for page_data in page_data_group:
                    if type(page_data) is dict and data_key:
                        values = page_data.get(data_key, [])
                        batch_data.extend(values)
                    elif type(page_data) is dict and not data_key:
                        values = page_data
                        batch_data.append(values)  # Ajouter le dictionnaire entier si data_key n'est pas spécifié
                    else:
                        values = page_data
                        batch_data.extend(values)

                if batch_data:
                    yield batch_data  # 🔥 Retourne le batch immédiatement au lieu d'attendre la fin

                if pagination_strategy.is_last_page(page_data_group, data_key):
                    has_data = False
                    break  # Arrêter si la pagination atteint la fin

        logger.info(f'{source_name} - Fetch completed')


class FocusApiConnector(ApiConnector):

    def __init__(self):

        api_key = ""
        api_key_name = "Ocp-Apim-Subscription-Key"

        self.headers = {
            "Content-Type": "application/json",
            f"{api_key_name}": api_key,
        }

    def get(self, url, **kwargs):

        return super().get(url, headers=self.headers, **kwargs)


class TribeApiConnector(ApiConnector):

    _client_id = ''
    _client_secret = ''
    _redirect_uri = 'TRIBE_URL/oauth2-request-refresh-token'
    _token_url = 'TRIBE_AUTH_URL/oauth2/token'
    _api_base_url = 'TRIBE_URL/api/v1'
    _s3 = boto3.client("s3")

    def __init__(self, env, token_s3_bucket, token_s3_key, force_with_token=None):
        self.env = env
        self.token_s3_bucket = token_s3_bucket
        self.token_s3_key = token_s3_key
        self.force_with_token = force_with_token

        self.init_refresh_token()

    def refresh_access_token(self, refresh_token):
        data = {
            'grant_type': 'refresh_token',
            'client_id': self._client_id,
            'client_secret': self._client_secret,
            'refresh_token': refresh_token
        }
        response = requests.post(self._token_url, data=data)
        response_data = response.json()
        logger.info(f"Obtained new token from refresh - {response_data}")
        return response_data['access_token'], response_data['refresh_token']

    def get_refresh_token(self):

        response = self._s3.get_object(Bucket=self.token_s3_bucket, Key=self.token_s3_key)
        refresh_token = response['Body'].read()

        return refresh_token

    def update_refresh_token(self, content):
        self._s3.put_object(Bucket=self.token_s3_bucket, Key=self.token_s3_key, Body=content)

    def init_refresh_token(self):

        try:
            if self.force_with_token:
                old_refresh_token = self.force_with_token
                logger.info(f"Forced old refresh token to {old_refresh_token}")
            else:
                old_refresh_token = self.get_refresh_token()
                logger.info(f"Got old refresh token {old_refresh_token}")

            logger.info("Getting new access token and refresh token from old regresh token")
            access_token, new_refresh_token = self.refresh_access_token(old_refresh_token)

            logger.info("Update old refresh token with the new one in s3")

            self.update_refresh_token(new_refresh_token)

            self.headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {access_token}",
            }

        except Exception as e:

            logger.error(f"TOKEN ERROR - Could not fetch the API using the current token {old_refresh_token} - {e}")

            # Paramètres OAuth2
            params = {
                "client_id": self._client_id,
                "redirect_uri": self._redirect_uri,
                "response_type": "code",
                "scope": "read write offline",
                "state": secrets.token_urlsafe(16)
            }

            # URL d'authentification
            auth_url = "https://app.tribecrm.nl/oauth2/authorize"

            # Rediriger l'utilisateur vers l'URL d'authentification
            logger.error(f"TOKEN ERROR - Please ask your admin to generate a new token using this link : {auth_url}?{requests.compat.urlencode(params)}")

    def get(self, url, **kwargs):

        return super().get(url, headers=self.headers, **kwargs)

    def check_token_or_reinitialize(self, url, params):
        logger.info("Check if current token is correct..")
        data = self.get(url=url, params=params)
        if data:
            logger.info(f"Current Token is correct - {len(data)=} - current_token = {self.get_refresh_token()}")
        else:
            logger.info(f"Current token is not valid - {len(data)=} - reinitializing token - current_token = {self.get_refresh_token()}")
            self.init_refresh_token()

        return self


class AfasApiConnector(ApiConnector):

    def __init__(self):

        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Accept-language": "nl-nl",
            "Authorization": "AfasToken PHRv***=="
        }

    def get(self, url, **kwargs):

        return super().get(url, headers=self.headers, **kwargs)


class NetsuiteApiConnector(ApiConnector):

    def __init__(self):

        self._private_key = """"""

        self._client_id = ""
        self._client_secret = ""
        self._certificate_id = ""
        self._url = "NETSUITE_API_URL"

        self._now = int(time.time())

        self._jwt_payload = {
            "iss": self._client_id,
            "scope": "rest_webservices,restlets",
            "aud": f"{self._url}/services/rest/auth/oauth2/v1/token",
            "iat": self._now,
            "exp": self._now + 3600
        }

        self._jwt_headers = {
            "typ": "JWT",
            "alg": "ES256",
            "kid": self._certificate_id
        }

        # Génération du token JWT
        self._jwt_token = jwt.encode(
            self._jwt_payload,
            self._private_key,
            algorithm="ES256",
            headers=self._jwt_headers
        )

        data = {
            "grant_type": "client_credentials",
            "client_assertion_type": "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
            "client_assertion": self._jwt_token
        }

        response = requests.post(
            url=f"{self._url}/services/rest/auth/oauth2/v1/token",
            data=data
        )

        token = response.json()

        self._headers = {
            "Authorization": f"Bearer {token['access_token']}",
            "prefer": "transient",
            "Content-type": "application/json"
        }

    def get(self, url, method="POST", **kwargs):

        return super().get(url, method=method, headers=self._headers, **kwargs)
