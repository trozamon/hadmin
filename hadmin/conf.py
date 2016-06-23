"""
HAdmin Configuration
--------------------

Manipulate hadmin's configuration

"""


from hadmin.yarn import Queue
import os
import yaml


class QueueGenerator:
    """
    Generate CapacityScheduler queues with YAML!
    """

    LONG_NAME_KEY = '__long_name'
    DONE_KEY = '__done'

    ADMINS_KEY = 'admins'
    USERS_KEY = 'users'
    RUNNING_KEY = 'running'
    CAPACITY_ROOT_KEY = 'capacity'
    CAPACITY_MAX_KEY = 'max'
    CAPACITY_WEIGHT_KEY = 'weight'
    USER_LIMIT_FACTOR_KEY = 'user_limit'

    def __init__(self, queue_confs):
        self._inputs = queue_confs

        for i in self._inputs:
            if i[QueueGenerator.LONG_NAME_KEY][0:6] != 'root.':
                tmp = 'root.' + i[QueueGenerator.LONG_NAME_KEY]
                i[QueueGenerator.LONG_NAME_KEY] = tmp

    @classmethod
    def get_parent_name(cls, queue_full_name):
        if '.' not in queue_full_name:
            return 'root'

        return '.'.join(queue_full_name.split('.')[0:-1])

    @classmethod
    def load_dir(cls, directory):
        objs = []

        for fname in os.listdir(directory):
            if fname[-4:] != '.yml':
                continue

            raw = ''

            with open(os.path.join(directory, fname), 'r') as f:
                queue_name = fname.replace('.yml', '')

                try:
                    raw = f.read()
                except UnicodeDecodeError as e:
                    e.reason += " in file " + fname
                    raise e

            obj = yaml.load(raw)
            obj[QueueGenerator.LONG_NAME_KEY] = queue_name
            obj[QueueGenerator.DONE_KEY] = False
            objs.append(obj)

        return QueueGenerator(objs)

    def generate(self):
        self.root = self.generate_queue(self._inputs, 'root')

        total_weights = self.compute_total_weights(self._inputs)

        def get_queue(fqn):
            if fqn == 'root':
                return self.root

            parts = fqn.split('.')
            i = 0
            if parts[0] == 'root':
                i = 1

            q = self.root
            for part in parts[i:]:
                q = q.subqueue(part)

            return q

        # Go through queues that are one level deep, two levels, deep, and so
        # on
        level = 0
        all_done = False

        while not all_done and level < 100:
            all_done = True

            for obj in self._inputs:
                name = obj[QueueGenerator.LONG_NAME_KEY]
                par_name = QueueGenerator.get_parent_name(name)

                if len(par_name.split('.')) == level:
                    q = self.generate_queue(self._inputs, name,
                                            total_weights[par_name])
                    get_queue(par_name).subqueues.append(q)

                all_done = all_done and obj[QueueGenerator.DONE_KEY]

            level += 1

        return self.root

    def compute_total_weights(self, inputs):
        weights = {
                'root': 0.0
                }

        for obj in inputs:
            key = QueueGenerator.get_parent_name(
                    obj[QueueGenerator.LONG_NAME_KEY])

            if key not in weights.keys():
                weights[key] = 0.0

            cap = obj[QueueGenerator.CAPACITY_ROOT_KEY]
            weight = cap[QueueGenerator.CAPACITY_WEIGHT_KEY]
            weights[key] += weight

        return weights

    def generate_queue(self, inputs, long_name, total_weight=1.0):
        """
        Generate a queue from an input

        total_weights should be the total of all the weights for a given level
        of queue. For example, if root.dev has a weight of 1 and root.prod has
        a weight of 2, total_weight passed in for generating both root.dev and
        root.prod should be 3.
        """

        for obj in inputs:
            if obj[QueueGenerator.LONG_NAME_KEY] == long_name:
                obj[QueueGenerator.DONE_KEY] = True
                q = Queue(
                        name=obj[QueueGenerator.LONG_NAME_KEY].split('.')[-1],
                        users=obj[QueueGenerator.USERS_KEY],
                        admins=obj[QueueGenerator.ADMINS_KEY],
                        running=obj[QueueGenerator.RUNNING_KEY]
                        )

                caps = obj[QueueGenerator.CAPACITY_ROOT_KEY]
                weight = caps[QueueGenerator.CAPACITY_WEIGHT_KEY]
                q.cap_min = (weight / total_weight) * 100.0
                q.cap_max = caps[QueueGenerator.CAPACITY_MAX_KEY]

                q.user_limit_factor = obj[QueueGenerator.USER_LIMIT_FACTOR_KEY]
                q.user_limit_factor /= q.cap_min

                return q

        return Queue(name=long_name.split('.')[-1])
