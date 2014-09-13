from flask import request, current_app

import flask_pymongo
import re


class Utils(object):

    def __init__(self):
        pass

    def get_sort(self, default_sort_string, apply_default=False):
        ''' Build the sort array to be applied on the query.
        Reads the request's 'sort' URL parameter to determine what should the sorting be.
        If there's no 'sort' URL parameter in then reteturn a default sort array.
        :param default_sort_string: the sort definitiosn we want to default to ince case there is no 'sort' request parameter.
        :param apply_default: ignore the 'sort' request paramter and just directly apply the give default sort definition.
        '''

        # Use regex to validate sorting string.
        # e.g. 'mp.slug asc' is OK but 'mp slug asc' or 'mp ascending' are not OK.
        sort_regex = re.compile('^([a-z]+(\.[a-z]+){0,})( desc| asc){0,1}$')

        # This flags tells us if we used the default sort rules.
        # If we didn't and the given sort rules triggers an error, then
        # We can try an call this function again with the default sort rules.
        using_default_sort_string = False

        # The array of sorting rules
        sort = []

        # Get sorting definition set as a value
        # of the 'sort' URL parameter in the request.
        # If no sort param has been given, use a default one.
        if not apply_default:
            sort_param = request.args.get('sort', default_sort_string)
        else:
            sort_param = default_sort_string

        if sort_param == default_sort_string:
            using_default_sort_string = True

        # Get all the sort definitions listed in the 'sort' URL parameter
        sort_definitions = sort_param.split(',')

        # For each of these sort definitions
        # define the sort object that will be appended in the sort array.
        for sort_definition in sort_definitions:
            match = sort_regex.match(sort_definition)

            if match:

                # The sort_rules variable is an array
                # that represents the property to sort on and the order.
                sort_rules = sort_definition.split(' ')

                # The property to sort on.
                sort_property = sort_rules[0]

                # The order to sort in. The default order is Ascending.
                sort_order = sort_rules[1] if len(sort_rules) == 2 else 'asc'

                # Build the sort object. The default order is Ascending.
                sort_object = (
                    sort_property,
                    flask_pymongo.DESCENDING if sort_order == 'desc' else flask_pymongo.ASCENDING
                )

                # Put the sort object in the array.
                sort.append(sort_object)

            elif not using_default_sort_string:
                error_msg = "Invalid '%s' sort definition, applying default definitions: '%s'" % (sort_definition, default_sort_string)
                current_app.logger.error(error_msg)
                return self.get_sort(default_sort_string, True)

        return sort
