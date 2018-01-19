# -*- coding: utf-8 -*-
"""Assets Module.

This module mirrors the Assets API.
"""
import cognite.config as config
import cognite._utils as _utils
from cognite.data_objects import AssetSearchObject

# Author: TK
def search_assets(description, api_key=None, project=None):
    '''Returns assets matching provided description.

    Args:
        description (str): Search query.

        api_key (str):          Your api-key.

        project (str):          Project name.
    Returns:
        AssetSearchObject
    '''
    api_key, project = config.get_config_variables(api_key, project)
    url = config.get_base_url() + '/projects/{}/assets'.format(project)
    params = {
        'description': description,
    }
    headers = {
        'api-key': api_key,
        'accept': 'application/json'
    }
    res = _utils.get_request(url, params=params, headers=headers)

    return AssetSearchObject(res.json())


# Author: TK
def get_assets(asset_id=None, depth=None, api_key=None, project=None):
    '''Returns assets with provided assetId.

    TODO:
        * TK: Enter description, args, and returns
    '''
    api_key, project = config.get_config_variables(api_key, project)
    url = config.get_base_url() + '/projects/{}/assets/{}'.format(project, asset_id)
    params = {
        'depth': depth,
    }
    headers = {
        'api-key': api_key,
        'accept': 'application/json'
    }
    res = _utils.get_request(url, params=params, headers=headers)
    return AssetSearchObject(res.json())
