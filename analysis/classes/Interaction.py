import sys
import numpy as np

from typing import Counter, List, Union, Dict
from collections import OrderedDict, Counter, defaultdict
from functools import cached_property

from . import Particle
from mlreco.utils.globals import PID_LABELS


class Interaction:
    """
    Data structure for managing interaction-level
    full chain output information.

    Attributes
    ----------
    id : int, default -1
        Unique ID (Interaction ID) of this interaction.
    particle_ids : np.ndarray, default np.array([])
        List of Particle IDs that make up this interaction
    num_particles: int, default 0
        Total number of particles in this interaction
    num_primaries: int, default 0
        Total number of primary particles in this interaction
    nu_id : int, default -1
        ID of the particle's parent neutrino
    volume_id : int, default -1
        ID of the detector volume the interaction lives in
    image_id : int, default -1
        ID of the image the interaction lives in
    index : np.ndarray, default np.array([])
        (N) IDs of voxels that correspondn to the particle within the image coordinate tensor that
    points : np.dnarray, default np.array([], shape=(0,3))
        (N,3) Set of voxel coordinates that make up this interaction in the input tensor
    vertex : np.ndarray, optional
        3D coordinates of the predicted interaction vertex
        in reconstruction (used for debugging)
    """
    def __init__(self,
                 interaction_id: int = -1,
                 particles: List[Particle] = None,
                 nu_id: int = -1,
                 volume_id: int = -1,
                 image_id: int = -1,
                 vertex: np.ndarray = -np.ones(3, dtype=np.float32),
                 is_neutrino: bool = False,
                 index: np.ndarray = np.empty(0, dtype=np.int64),
                 points: np.ndarray = np.empty((0,3), dtype=np.float32),
                 depositions: np.ndarray = np.empty(0, dtype=np.float32),
                 crthit_matched: bool = False,
                 crthit_matched_particle_id: int = -1,
                 crthit_id: int = -1,
                 flash_time: float = -float(sys.maxsize),
                 fmatched: bool = False,
                 flash_total_pE: float = -1,
                 flash_id: int = -1,
                 flash_hypothesis: float = -1,
                 matched: bool = False,
                 is_contained: bool = False):

        # Initialize attributes
        self.id           = int(interaction_id)
        self.nu_id        = int(nu_id)
        self.volume_id    = int(volume_id)
        self.image_id     = int(image_id)
        self.vertex       = vertex
        self.is_neutrino  = is_neutrino
        
        # Initialize private attributes to be set by setter only
        self._particles  = None
        self._size       = None
        # Invoke particles setter
        self._particle_counts = np.zeros(6, dtype=np.int64)
        self._primary_counts  = np.zeros(6, dtype=np.int64)
        self.particles   = particles

        # Aggregate individual particle information 
        if self._particles is None:
            self._particle_ids   = np.empty(0, dtype=np.int64)
            self._num_particles  = 0
            self._num_primaries  = 0
            self.index           = np.atleast_1d(index)
            self.points          = np.atleast_1d(points)
            self.depositions     = np.atleast_1d(depositions)
            self._particles      = particles

        # Quantities to be set by the particle matcher
        self._match         = []
        self._match_counts  = OrderedDict()
        self.matched        = matched
        self._is_principal_match = False
        
        self.is_contained   = is_contained
        
        # Flash matching quantities
        self.flash_time     = flash_time
        self.fmatched       = fmatched
        self.flash_total_pE = flash_total_pE
        self.flash_id       = flash_id
        self.flash_hypothesis = flash_hypothesis

        # CRT-TPC matching quantities
        self.crthit_matched = crthit_matched
        self.crthit_matched_particle_id = crthit_matched_particle_id
        self.crthit_id = crthit_id
        
    @property
    def size(self):
        if self._size is None:
            self._size = len(self.index)
        return self._size
        
    @property
    def match(self):
        return np.array(list(self._match_counts.keys()), dtype=np.int64)
    
    @property
    def match_counts(self):
        return np.array(list(self._match_counts.values()), dtype=np.float32)
    
    @property
    def is_principal_match(self):
        return self._is_principal_match
        
    @classmethod
    def from_particles(cls, particles, verbose=False, **kwargs):
        
        assert len(particles) > 0
        init_args = defaultdict(list)
        reserved_attributes = [
            'interaction_id', 'nu_id', 'volume_id', 
            'image_id', 'points', 'index', 'depositions'
        ]
        
        processed_args = {'particles': []}
        for key, val in kwargs.items():
            processed_args[key] = val
            
        for p in particles:
            assert type(p) is Particle
            for key in reserved_attributes:
                if key not in kwargs:
                    init_args[key].append(getattr(p, key))
            processed_args['particles'].append(p)
        
        _process_interaction_attributes(init_args, processed_args, **kwargs)
        
        interaction = cls(**processed_args)
        return interaction
        

    def check_particle_input(self, x):
        """
        Consistency check for particle interaction id and self.id
        """
        assert type(x) is Particle
        assert x.interaction_id == self.id

    @property
    def particles(self):
        return self._particles.values()

    @particles.setter
    def particles(self, value):
        '''
        <Particle> list getter/setter. The setter also sets
        the general interaction properties
        '''
        assert isinstance(value, list)
        
        if self._particles is not None:
            msg = f"Interaction {self.id} already has a populated list of "\
                "particles. You cannot change the list of particles in a "\
                "given Interaction once it has been set."
            raise AttributeError(msg)

        if value is not None:
            self._particles = {p.id : p for p in value}
            self._particle_ids = np.array(list(self._particles.keys()), 
                                          dtype=np.int64)
            id_list, index_list, points_list, depositions_list = [], [], [], []
            for p in value:
                self.check_particle_input(p)
                id_list.append(p.id)
                index_list.append(p.index)
                points_list.append(p.points)
                depositions_list.append(p.depositions)
                if p.pid >= 0:
                    self._particle_counts[p.pid] += 1
                    self._primary_counts[p.pid] += int(p.is_primary)
                else:
                    self._particle_counts[-1] += 1
                    self._primary_counts[-1] += int(p.is_primary)

            # self._particle_ids = np.array(id_list, dtype=np.int64)
            self._num_particles = len(value)
            self._num_primaries = len([1 for p in value if p.is_primary])
            self.index = np.atleast_1d(np.concatenate(index_list))
            self.points = np.vstack(points_list)
            self.depositions = np.atleast_1d(np.concatenate(depositions_list))
            
    def _update_particle_info(self):
        self._particle_counts = np.zeros(6, dtype=np.int64)
        self._primary_counts  = np.zeros(6, dtype=np.int64)
        for p in self.particles:
            if p.pid >= 0:
                self._particle_counts[p.pid] += 1
                self._primary_counts[p.pid] += int(p.is_primary)
            else:
                self._particle_counts[-1] += 1
                self._primary_counts[-1] += int(p.is_primary)
            self._num_particles = len(self.particles)
            self._num_primaries = len([1 for p in self.particles if p.is_primary])
        
    @property
    def particle_ids(self):
        return self._particle_ids
    
    @particle_ids.setter
    def particle_ids(self, value):
        # If particles exist as attribute, disallow manual assignment
        assert self._particles is None
        self._particle_ids = value
        
    @property
    def particle_counts(self):
        return self._particle_counts
        
    @property
    def primary_counts(self):
        return self._primary_counts
        
    @property
    def num_primaries(self):
        return self._num_primaries
    
    @property
    def num_particles(self):
        return self._num_particles

    def __getitem__(self, key):
        if self._particles is None:
            msg = "You can't access member particles of an interactions by "\
                "__getitem__ method if <Particle> instances are missing. "\
                "Either initialize Interactions with the <from_particles> "\
                "constructor or manually assign particles. "
            raise KeyError(msg)
        return self._particles[key]

    def __repr__(self):
        return f"Interaction(id={self.id:<3}, vertex={str(self.vertex)}, nu_id={self.nu_id}, size={self.size:<4}, Topology={self.topology})"

    def __str__(self):
        msg = "Interaction {}, Vertex: x={:.2f}, y={:.2f}, z={:.2f}\n"\
            "--------------------------------------------------------------------\n".format(
            self.id, self.vertex[0], self.vertex[1], self.vertex[2])
        return msg + self.particles_summary
    
    @property
    def topology(self):
        msg = ""
        encode = {0: 'g', 1: 'e', 2: 'mu', 3: 'pi', 4: 'p', 5: '?'}
        for i, count in enumerate(self._primary_counts):
            if count > 0:
                msg += f"{count}{encode[i]}"
        return msg

    @property
    def particles_summary(self):

        primary_str = {True: '*', False: '-'}
        self._particles_summary = ""
        if self._particles is None: return
        for p in sorted(self._particles.values(), key=lambda x: x.is_primary, reverse=True):
            pmsg = "    {} Particle {}: PID = {}, Size = {}, Match = {} \n".format(
                primary_str[p.is_primary], p.id, p.pid, p.size, str(p.match))
            self._particles_summary += pmsg
        return self._particles_summary


# ------------------------------Helper Functions---------------------------

def _process_interaction_attributes(init_args, processed_args, **kwargs):
    
    # Interaction ID
    if 'interaction_id' not in kwargs:
        int_id, counts = np.unique(init_args['interaction_id'], 
                                    return_counts=True)
        int_id = int_id[np.argsort(counts)[::-1]]
        if len(int_id) > 1:
            msg = "When constructing interaction {} from list of its "\
                "constituent particles, encountered non-unique interaction "\
                "id: {}".format(int_id[0], str(int_id))
            raise AssertionError(msg)
        processed_args['interaction_id'] = int_id[0]
    else:
        processed_args['interaction_id'] = kwargs['interaction_id']
    
    if 'nu_id' not in kwargs:
        nu_id, counts = np.unique(init_args['nu_id'], return_counts=True)
        processed_args['nu_id'] = nu_id[np.argmax(counts)]
    else:
        processed_args['nu_id'] = kwargs['nu_id']
    
    if 'volume_id' not in kwargs:
        volume_id, counts = np.unique(init_args['volume_id'], 
                                        return_counts=True)
        processed_args['volume_id'] = volume_id[np.argmax(counts)]
    else:
        processed_args['volume_id'] = kwargs['volume_id']
    
    if 'image_id' not in kwargs:
        image_id, counts = np.unique(init_args['image_id'], return_counts=True)
        processed_args['image_id'] = image_id[np.argmax(counts)]
    else:
        processed_args['image_id'] = kwargs['image_id']
    
    if len(init_args['index']) > 0:
        processed_args['points'] = np.vstack(init_args['points'])
        processed_args['index'] = np.concatenate(init_args['index'])
        processed_args['depositions'] = np.concatenate(init_args['depositions'])
    else:
        processed_args['points'] = np.empty(0, dtype=np.float32)
        processed_args['index'] = np.empty(0, dtype=np.int64)
        processed_args['depositions'] = np.empty(0, dtype=np.float32)
