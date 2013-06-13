def get_sqlite3_hex_convert_expr(hex_str, maxlen=16):
    """
    Given a part of SQL query returning a text string,
    this function returns an SQLite3-compatible expression
    that converts this text string from a hexadecimal (uppercase)
    to a number.

    >>> # 48350 actually
    >>> get_sqlite3_hex_convert_expr("'BCDE'",
    ...                              4) #doctest:+NORMALIZE_WHITESPACE
    "(4096 * (CASE substr('BCDE', 1, 1) WHEN '0' THEN 0 WHEN '1' THEN 1
    WHEN '2' THEN 2 WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
    WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8 WHEN '9' THEN 9
    WHEN 'A' THEN 10 WHEN 'B' THEN 11 WHEN 'C' THEN 12 WHEN 'D' THEN 13
    WHEN 'E' THEN 14 ELSE 15 END)
    + 256 * (CASE substr('BCDE', 2, 1) WHEN '0' THEN 0 WHEN '1' THEN 1
    WHEN '2' THEN 2 WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
    WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8 WHEN '9' THEN 9
    WHEN 'A' THEN 10 WHEN 'B' THEN 11 WHEN 'C' THEN 12 WHEN 'D' THEN 13
    WHEN 'E' THEN 14 ELSE 15 END)
    + 16 * (CASE substr('BCDE', 3, 1) WHEN '0' THEN 0 WHEN '1' THEN 1
    WHEN '2' THEN 2 WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
    WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8 WHEN '9' THEN 9
    WHEN 'A' THEN 10 WHEN 'B' THEN 11 WHEN 'C' THEN 12 WHEN 'D' THEN 13
    WHEN 'E' THEN 14 ELSE 15 END)
    + 1 * (CASE substr('BCDE', 4, 1) WHEN '0' THEN 0 WHEN '1' THEN 1
    WHEN '2' THEN 2 WHEN '3' THEN 3 WHEN '4' THEN 4 WHEN '5' THEN 5
    WHEN '6' THEN 6 WHEN '7' THEN 7 WHEN '8' THEN 8 WHEN '9' THEN 9
    WHEN 'A' THEN 10 WHEN 'B' THEN 11 WHEN 'C' THEN 12 WHEN 'D' THEN 13
    WHEN 'E' THEN 14 ELSE 15 END))"
    """
    whens = ' '.join("WHEN '{0:X}' THEN {0}".format(i)
                         for i in xrange(15))\
            + ' ELSE 15'

    result = ('{power} * (CASE substr({input}, {i}, 1) {whens} END)'
                  .format(power=0x10 ** (maxlen - 1 - i),
                          input=hex_str,
                          i=i + 1,
                          whens=whens)
                  for i in xrange(maxlen))

    return '({})'.format(' + '.join(result))


def get_postgresql_hex_convert_expr(hex_str, maxlen=8):
    r"""
    Given a part of SQL query returning a bytea string,
    this function returns an PostgreSQL-compatible expression
    that converts this bytea string to a number.

    >>> get_postgresql_hex_convert_expr(
    ...     'fingerprint',
    ... 8) #doctest:+NORMALIZE_WHITESPACE
    '(72057594037927936 * get_byte(fingerprint, 0)
    + 281474976710656 * get_byte(fingerprint, 1)
    + 1099511627776 * get_byte(fingerprint, 2)
    + 4294967296 * get_byte(fingerprint, 3)
    + 16777216 * get_byte(fingerprint, 4)
    + 65536 * get_byte(fingerprint, 5)
    + 256 * get_byte(fingerprint, 6)
    + 1 * get_byte(fingerprint, 7))'

    >>> # 2237085 actually
    >>> get_postgresql_hex_convert_expr(
    ...     r"E'\\000\\000\\000\\000\\000" r'""' r"\\235'::bytea",
    ...     8) #doctest:+NORMALIZE_WHITESPACE
    '(72057594037927936 *
        get_byte(E\'\\\\000\\\\000\\\\000\\\\000\\\\000""\\\\235\'::bytea, 0)
    + 281474976710656 *
        get_byte(E\'\\\\000\\\\000\\\\000\\\\000\\\\000""\\\\235\'::bytea, 1)
    + 1099511627776 *
        get_byte(E\'\\\\000\\\\000\\\\000\\\\000\\\\000""\\\\235\'::bytea, 2)
    + 4294967296 *
        get_byte(E\'\\\\000\\\\000\\\\000\\\\000\\\\000""\\\\235\'::bytea, 3)
    + 16777216 *
        get_byte(E\'\\\\000\\\\000\\\\000\\\\000\\\\000""\\\\235\'::bytea, 4)
    + 65536 *
        get_byte(E\'\\\\000\\\\000\\\\000\\\\000\\\\000""\\\\235\'::bytea, 5)
    + 256 *
        get_byte(E\'\\\\000\\\\000\\\\000\\\\000\\\\000""\\\\235\'::bytea, 6)
    + 1 *
        get_byte(E\'\\\\000\\\\000\\\\000\\\\000\\\\000""\\\\235\'::bytea, 7))'
    """
    result = ('{power} * get_byte({input}, {i})'
                  .format(power=0x100 ** (maxlen - 1 - i),
                          input=hex_str,
                          i=i)
                  for i in xrange(maxlen))
    return '({})'.format(' + '.join(result))
