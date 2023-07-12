import os
import numpy as np
import yaml
import pickle
from collections import defaultdict
from analysis.post_processing import post_processing
from mlreco.utils.globals import *
#from matcha.match_candidate import MatchCandidate

@post_processing(data_capture=['index', 'crthits'], 
                 result_capture=['interactions'])
def run_crt_tpc_matching(data_dict, result_dict, 
                         crt_tpc_manager=None,
                         crthit_keys=[]):
    """
    Post processor for running CRT-TPC matching using matcha.
    
    Parameters
    ----------

    Returns
    -------
    update_dict: dict of list
        Dictionary of a list of length batch_size, where each entry in 
        the list is a mapping:
            interaction_id : (matcha.CRTHit, matcha.MatchCandidate)
        
    NOTE: This post-processor also modifies the list of Interactions
    in-place by adding the following attributes:
        interaction.crthit_matched: (bool)
            Indicator for whether the given interaction has a CRT-TPC match
        interaction.crthit_id: (list of ints)
            List of IDs for CRT hits that were matched to one or more tracks
    """
    print('Running CRT matching...')
    from matcha.match_candidate import MatchCandidate

    crthits = {}
    assert len(crthit_keys) > 0
    for key in crthit_keys:
        crthits[key] = data_dict[key]
    
    interactions = result_dict['interactions']
    entry        = data_dict['index']
    
    crt_tpc_matches = crt_tpc_manager.get_crt_tpc_matches(int(entry), 
                                                          interactions,
                                                          crthits,
                                                          use_true_tpc_objects=False,
                                                          restrict_interactions=[])

    # Get matcha output file path from the matcha config file. Load the relevant
    # class instances from that file.
    matcha_config_path = crt_tpc_manager.crt_tpc_config['matcha_config_path']
    with open(matcha_config_path, 'r') as file:
        matcha_config = yaml.safe_load(file)

    matcha_file_save_config = matcha_config['file_save_config']
    file_path = matcha_file_save_config['save_file_path']
    file_name = matcha_file_save_config['save_file_name']
    matcha_output_file_path = file_path + file_name
    if not os.path.exists(matcha_output_file_path):
        raise FileNotFoundError("""
            matcha output file '{:s}' does not exist.\n
            Make sure to set a valid save_file_path and save_file_name in your matcha config file.
        """).format(matcha_output_file_path)

    with open(matcha_output_file_path, 'rb') as file:
        matcha_output_dict = pickle.load(file)
        track_list = matcha_output_dict['tracks']
        crthist_list = matcha_output_dict['crthits']
        match_list = matcha_output_dict['match_candidates']

    assert all(isinstance(match, MatchCandidate) for match in match_list)

    for match in match_list:
        matched_track_id = match.track_id
        matched_track = None
        for track in track_list:
            if track.id == matched_track_id:
                matched_track = track
                break
        if matched_track is None: continue

        matched_interaction = None
        for interaction in interactions:
            if matched_track.interaction_id == interaction.id:
                matched_interaction = interaction
                break
        if matched_interaction is None: continue

        matched_crthit_id = match.crthit_id
        matched_interaction.crthit_matched = True
        matched_interaction.crthit_matched_particle_id = matched_track.id
        matched_interaction.crthit_id = matched_crthit_id

        # update_dict['interactions'].append(matched_interaction)
    # update_dict['crt_tpc_matches'].append(crt_tpc_dict)
    print('Done CRT matching.')
    return {}






