# ----------------------------------------------------------------------------
# Copyright (c) 2016-2017, QIIME 2 development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file LICENSE, distributed with this software.
# ----------------------------------------------------------------------------

# This code was sourced and adapted on 4/5/17 from:
# https://github.com/qiime2/qiime2/blob/qiime2/metadata.py

# The license for this code can be found within the licenses/ directory.

# On adaptation, a few important components of the QIIME2 object were removed.
# Specifically, we no longer support load from a file. This is fine as we're
# controlling the loading the metadata explicitly already. Second, we removed
# the ability to automatically generate a index name as it is not necessary
# within redbiom as we already strongly control the index. Last, we removed
# the category selection and its corresponding object as that was not
# necessary for use within redbiom. The tests pulled over were adapted as
# needed.

import sqlite3


class Metadata:
    def __init__(self, dataframe):
        self._dataframe = dataframe

    def ids(self, where=None):
        """Retrieve IDs matching search criteria.

        Parameters
        ----------
        where : str, optional
            SQLite WHERE clause specifying criteria IDs must meet to be
            included in the results. All IDs are included by default.

        Returns
        -------
        set
            IDs matching search criteria specified in `where`.

        """
        if where is None:
            return set(self._dataframe.index)

        conn = sqlite3.connect(':memory:')
        conn.row_factory = lambda cursor, row: row[0]

        # If the index isn't named, generate a unique random column name to
        # store it under in the SQL table. If we don't supply a column name for
        # the unnamed index, pandas will choose the name 'index', and if that
        # name conflicts with existing columns, the name will be 'level_0',
        # 'level_1', etc. Instead of trying to guess what pandas named the
        # index column (since this isn't documented behavior), explicitly
        # generate an index column name.
        index_column = self._dataframe.index.name
        self._dataframe.to_sql('metadata', conn, index=True,
                               index_label=index_column)

        c = conn.cursor()

        # In general we wouldn't want to format our query in this way because
        # it leaves us open to sql injection, but it seems acceptable here for
        # a few reasons:
        # 1) This is a throw-away database which we're just creating to have
        #    access to the query language, so any malicious behavior wouldn't
        #    impact any data that isn't temporary
        # 2) The substitution syntax recommended in the docs doesn't allow
        #    us to specify complex `where` statements, which is what we need to
        #    do here. For example, we need to specify things like:
        #        WHERE Subject='subject-1' AND SampleType='gut'
        #    but their qmark/named-style syntaxes only supports substition of
        #    variables, such as:
        #        WHERE Subject=?
        # 3) sqlite3.Cursor.execute will only execute a single statement so
        #    inserting multiple statements
        #    (e.g., "Subject='subject-1'; DROP...") will result in an
        #    OperationalError being raised.
        query = ('SELECT "{0}" FROM metadata WHERE {1} GROUP BY "{0}" '
                 'ORDER BY "{0}";'.format(index_column, where))

        try:
            c.execute(query)
        except sqlite3.OperationalError:
            conn.close()
            raise ValueError("Selection of IDs failed with query:\n %s"
                             % query)

        ids = set(c.fetchall())
        conn.close()
        return ids


def samples(sample_values, criteria):
    """Select samples based on specified criteria

    Parameters
    ----------
    sample_values : pandas.Series
        A series indexed by the Sample ID and valued by something.
    criteria : str
        Selection criteria. Simple logic can be specified, but cannot be
        chained. The following operators are available:

            {<, >, in, notin}

        For example, to keep samples with a value less than 5, the following
        form works: "< 5". To keep samples matching a discrete set of possible
        states, use the "in" operator and denote the valid states with a comma.
        Quotes are possible as well, for instance, "in foo,'bar baz" will keep
        samples whose value are either "foo" or "bar baz".

        If no operator is specified, it is assumed an exact string match of the
        value is to be performed.

    Returns
    -------
    generator of str
        Yields the sample IDs which meet the criteria.

    Raises
    ------
    ValueError
        If the criteria cannot be parsed.
        If the > or < operator is used, and the right hand side of the
            criteria do not appear to be numeric.
    """
    import shlex
    from redbiom.util import float_or_nan
    tokens = list(shlex.shlex(criteria))
    if len(tokens) > 1:
        # < 5
        # in "foo bar",blah
        # notin thing,"other thing"

        op = {'in': lambda a, b: a in b,
              'notin': lambda a, b: a not in b,
              '<': lambda a, b: a <= b,
              '>': lambda a, b: a >= b}
        operator = op.get(tokens[0])
        if operator is None:
            func = lambda to_test: to_test == criteria
        elif tokens[0] in ('in', 'notin'):
            rh = [t.strip("'").strip('"') for t in tokens[1:] if t != ',']
            tokens = set(rh)
            func = lambda to_test: operator(to_test, rh)
        else:
            rh = tokens[1]
            if len(tokens) > 2:
                raise ValueError("Unexpected criteria: %s" % criteria)
            try:
                rh = float(rh)
            except TypeError:
                if operator in {'<', '>'}:
                    raise ValueError("Right hand does not look numeric")

            func = lambda to_test: operator(float_or_nan(to_test), rh)
    else:
        func = lambda to_test: to_test == criteria

    for s, v in zip(sample_values.index, sample_values.values):
        if func(v):
            yield s
