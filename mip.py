class MultiPropInequality(object):
    """Wrapper class that handles inequalities operating on multiple properties.

    NOTE: GQL is not supported.

    The original result set returned by the underlying query will be filtered
    using post filters. Only those results that satisfy all of the post filters
    will be handed back.

    For better effeciency, set the first inequality filter that is most likely
    to return the least number of data.

    To use:

        query = MultiPropInequality(Model)

    or with search by ancestor:

        query = MultiPropInequality(Model.query(ancestor=key))

    or with pre-built query:

        query = Model.query().filter(Model.prop='value')
        query = MultiPropInequality(query)

    filter() method returns self, so chaining is posible:

        query = MultiPropInequality(Model) \
                    .filter(Model.prop1<='value2') \
                    .filter(Model.prop2!='value2')

    filter() method updates itself (instead of creating a new query), so there
    is no need to reassign the variable:

        query = query.filter(Model.prop1!=0)    # Not necessary

        query.filter(Model.prop1!=0)            # (better) query is updated

    To access the result set, iterate through:

        for result in query:
            # do something with result
    """

    def __init__(self, model_or_query):
        """Creates new instance of this class.

        Parameters:
            model_or_query - Required. An instance of a Model or a pre-built
                             Query object with at most 1 property that is being
                             evaluated for inequalities.
        """
        if isinstance(model_or_query, ndb.model.MetaModel):
            self.query = model_or_query.query()
        elif isinstance(model_or_query, ndb.query.Query):
            self.query = model_or_query
        else:
            raise TypeError(
                'Must be a subclass of Model or an instance of Query.'
            )

        self.first_inequality = self._get_first_inequality(self.query.filters)
        self.post_inq_filters = []


    def filter(self, *args):
        """Checks if each arg is an inequality filter and appends it to the list
        inequality post filters.

        If arg is an inquality filter (or at least one of its subnodes is an
        inequality filter), and operates on a property that is different from
        the first inequality, then it will be added to the list of post
        inequality filters. Otherwise, it will be passed on to the underlying
        query.

        Parameters:
            args - Optional. Node. Can be instances of any of the subclasess of
                   Node (e.g. FilterNode, ConjunctionNode, DisjunctionNode).

        Returns the instance of this class.
        """
        if not args:
            return self

        for arg in args:
            if not isinstance(arg, ndb.query.Node):
                raise TypeError('Filter should be instance of Node.')
            if not self._push_filter(arg):
                self.query = self.query.filter(arg)

        return self


    def _get_first_inequality(self, f_node):
        """Returns the first inquality of a given node. Returns None if no
        inequality can be found.
        """
        inequalities = self._get_inequalities(f_node)
        return None if len(inequalities) == 0 else inequalities[0]
        # if len(inequalities) == 0:
        #     return None
        # else:
        #     return inequalities[0]


    def _get_inequalities(self, f_node):
        """Returns a list of inequalities in a given node and all of its
        subnodes
        """
        inequalities = []
        if isinstance(f_node,
                (ndb.query.DisjunctionNode, ndb.query.ConjunctionNode)):
            for f_n in f_node:
               inequalities.extend(self._get_inequalities(f_n))
        elif isinstance(f_node, ndb.query.FilterNode):
            filter_dict = self._node_to_dict(f_node)
            if filter_dict['symbol'] != '=':
                inequalities.append(filter_dict)
        return inequalities


    def _push_filter(self, filter_node):
        """Adds filter_node to the list post inquality filters if filter_node
        has at least 1 inquality that operates on a property that is different
        from the first inequality.

        Returns True if filter_node is added to the list of post inquality
        filters. If filter_node is the first inequality, returns False.
        """
        inequalities = self._get_inequalities(filter_node)
        push_to_post = False

        for inq in inequalities:
            if not self.first_inequality:
                self.first_inequality = inq
            elif self.first_inequality['name'] != inq['name']:
                push_to_post = True
                break
            # else:
            #     self.first_inequality['name'] != inq['name']
            #     push_to_post = True
            #     break

        if push_to_post:
            self.post_inq_filters.append(filter_node)

        return push_to_post


    def _node_to_dict(self, f_node):
        """Converts a Filter to a dictionary"""
        filter_dict = {}
        node_dict = getattr(f_node, '__dict__')
        for key, value in node_dict.items():
            if key.endswith('name'):
                filter_dict['name'] = value
            elif key.endswith('symbol'):
                filter_dict['symbol'] = value
            elif key.endswith('value'):
                filter_dict['value'] = value
        return filter_dict


    def _make_evaluator(self, filter_node):
        """Returns an evaluator for a given FilterNode"""
        def make_closure(f_d):
            if (f_d['symbol'] == '>'):
                return lambda x: getattr(x, f_d['name']) > f_d['value']
            elif (f_d['symbol'] == '<'):
                return lambda x: getattr(x, f_d['name']) < f_d['value']
            elif (f_d['symbol'] == '>='):
                return lambda x: getattr(x, f_d['name']) >= f_d['value']
            elif (f_d['symbol'] == '<='):
                return lambda x: getattr(x, f_d['name']) <= f_d['value']
            elif (f_d['symbol'] == '!='):
                return lambda x: getattr(x, f_d['name']) != f_d['value']
            elif (f_d['symbol'] == '='):
                return lambda x: getattr(x, f_d['name']) == f_d['value']
            else:
                raise NotImplementedError(
                    'Unsuported operator: {}'.format(f_d['symbol'])
                )

        f_dict = self._node_to_dict(filter_node)

        # FilterNode coverts TimeProperty and DateProperty to datetime.
        # Convert back to time or date
        prop_type = ndb.Model._kind_map[self.query.kind] \
                        ._properties[f_dict['name']].__class__.__name__
        if prop_type == 'TimeProperty' \
                and isinstance(f_dict['value'], datetime):
            f_dict['value'] = f_dict['value'].time()
        elif prop_type == 'DateProperty' \
                and isinstance(f_dict['value'], datetime):
            f_dict['value'] = f_dict['value'].date()

        return make_closure(f_dict)


    def _make_and_evaluator(self, con_node):
        """Returns an evaluator that performs AND operation on subnodes of a
        given ConjunctionNode
        """
        post_evals = self._check_node(con_node)
        def and_evaluators(x):
            for p_eval in post_evals:
                if not p_eval(x):
                    return False
            return True

        return and_evaluators


    def _make_or_evaluator(self, dis_node):
        """Returns an evaluator that performs OR operation on subnodes of a
        given DisjunctionNode
        """
        post_evals = self._check_node(dis_node)
        def or_evaluators(x):
            for p_eval in post_evals:
                if p_eval(x):
                    return True
            return False

        return or_evaluators


    def _check_node(self, a_node):
        """Returns a list of evaluator functions from a given Node"""
        output = []
        for f_n in a_node:
            if isinstance(f_n, ndb.query.ConjunctionNode):
                output.append(self._make_and_evaluator(f_n))
            elif isinstance(f_n, ndb.query.DisjunctionNode):
                output.append(self._make_or_evaluator(f_n))
            elif isinstance(f_n, ndb.query.FilterNode):
                output.append(self._make_evaluator(f_n))
            # FalseNode is ignored
            # ParameterNode is not yet supported
        return output


    def __iter__(self):
        """Iterates through the result set of the underlying query, and hands
        back only those that satify all of the post inequality filters.
        """
        post_evaluator = self._make_and_evaluator(self.post_inq_filters)
        for result in self.query:
            try:
                if post_evaluator(result):
                    yield result
            except TypeError:
                pass            # Value of a property from datastore is None

MIP = MultiPropInequality       # Shorthand and prefered version
