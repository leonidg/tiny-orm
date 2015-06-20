import copy
import sqlite3
import threading

def ClassFactory(name, argument_typecasters, exceptions=[], BaseClass=object, **kwargs):
    """
    Some magic, inspired by jsbueno on StackOverflow
    http://stackoverflow.com/questions/15247075/how-can-i-dynamically-create-derived-classes-from-a-base-class

    This class allows us to create a class dynamically
    whose objects have a fixed set of legal attributes
    with static types:

    MyClass = ClassFactory("MyClass", {"a": str, "b": int, "c": int})
    obj = MyClass(a=True, b=False, c=4)
    obj.a     # "True"
    obj.b     # 0
    obj.c     # 4
    obj.d     # throws an AttributeError (as expected)
    m1.d = 5  # throws a TypeError
    """
    def __init__(self, **__init__kwargs):
        for key, value in __init__kwargs.items():
            setattr(self, key, value)
        BaseClass.__init__(self, **kwargs)
    def __setattr__(self, key, value):
        if key in exceptions:
            BaseClass.__setattr__(self, key, value)
        elif key not in argument_typecasters:
            raise TypeError("Argument %s not valid for %s" % (key, self.__class__.__name__))
        else:
            typecaster = argument_typecasters[key]
            object.__setattr__(self, key, typecaster(value))
    newclass = type(name, (BaseClass,), {"__init__": __init__, "__setattr__": __setattr__})
    return newclass


def NoneSafeType(_type):
    """
    A hack for a "None-safe" typecaster. Given a type, it casts all
    values to that type as the type would, except that None is always
    cast to None.
    """
    def caster(value):
        if value is None:
            return None
        else:
            return _type(value)
    return caster


class ORMBase(object):
    """
    This class serves as the base for each instance of a "mapped
    object," which is to say an object representing a row in a SQLite
    table.
    """
    id_key_name = "__id"
    def __init__(self, orm):
        self.__orm = orm
    def save(self):
        if not hasattr(self, ORMBase.id_key_name):
            # This object does not exist in the database.
            self.__orm.insert_row(self)
        else:
            # This object exists in database.
            self.__orm.update(self)


class ORM(object):
    """
    This class is the main ORM library. Instances of this class are
    callable objects, the return value of which is a mapped object.
    The instances of this class might have some of their own methods
    or properties, which conceptually correspond to the entire dataset
    (i.e. table). So for example:

       Person = ORM(...)
       johnDoe = Person(first_name="john", last_name="doe")

    Person is an object that conceptually represents the table of all
    people in the database (assuming that such a table exists). johnDoe is,
    in effect, a specific instance of Person. Thus, the return value of ORM
    is conceptually a class-like object.
    """

    # Mappings from Python types to SQLite types
    known_column_types = {
        int: "INTEGER",
        float: "REAL",
        bool: "BOOLEAN",
        str: "STRING",
    }

    def __init__(self, database_name, schema):
        if "table" not in schema:
            raise TypeError("Schema must contain table name (string).")
        if "columns" not in schema or type(schema["columns"]) is not dict:
            raise TypeError("Schema must contain column definitions (dict names -> types).")
        if ORMBase.id_key_name in schema["columns"].keys():
            raise TypeError("Cannot use %r as column name. It's reserved." % (ORMBase.id_key_name,))
        self.connection = sqlite3.connect(database_name, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection_lock = threading.RLock()
        self.table = schema["table"]
        self.columns = { ORMBase.id_key_name: { "type": int } }
        argument_typecasters = { ORMBase.id_key_name: NoneSafeType(int) }
        for column_name, column_definition in copy.deepcopy(schema["columns"]).items():
            # long form definition
            #    favorite_number: { "type": int, "default": 5 }
            if type(column_definition) is dict:
                if "type" not in column_definition:
                    raise TypeError("Column definition %s must have 'type' parameter." % (column_definition,))
                elif column_definition["type"] not in ORM.known_column_types:
                    raise TypeError("Illegal column type! Must be one of %s." % (ORM.known_column_types.keys(),))
                else:
                    column_type = NoneSafeType(column_definition["type"])
                    self.columns[column_name] = column_definition
            # short form definition
            #    favorite_number: int
            elif type(column_definition) is type:
                if column_definition not in ORM.known_column_types:
                    raise TypeError("Illegal column type! Must be one of %s." % (ORM.known_column_types.keys(),))
                column_type = NoneSafeType(column_definition)
                self.columns[column_name] = { "type": column_definition }
            argument_typecasters[column_name] = column_type
        self.sync() # Will raise TypeError if columns unsafe
        self.mapped_object = ClassFactory("%s_Mapper" % (self.table,),
                                          argument_typecasters=argument_typecasters,
                                          # These are magic internal names for objects that BaseClass will need
                                          exceptions=["_ORMBase__orm", "_ORM%s" % (ORMBase.id_key_name,) ],
                                          BaseClass=ORMBase,
                                          orm=self)

    def __call__(self, **kwargs):
        for column_name, column_definition in self.columns.items():
            if column_name == ORMBase.id_key_name:
                continue
            elif column_name not in kwargs:
                if "default" in column_definition:
                    kwargs[column_name] = column_definition["default"]
                else:
                    kwargs[column_name] = None
        return self.mapped_object(**kwargs)

    def execute_query(self, query, params=[]):
        with self.connection_lock:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            self.connection.commit()
            result = cursor.fetchall()
            cursor.close()
            return result

    def sync(self):
        """
        Multi-use function that tries to make sure the ORM and the SQLite table are in sync:
            - Creates table if it doesn't exist
            - If table exists and its columns are a subset of the given columns,
              adds the additional columns.
            - If table exists and contains columns not in the given columns, prints a warning
              and ignores the extra columns.
            - If table exists and contains a column with the same name but different definition
              (type or default value) as one of the given ones, raises an exception.
        """
        rows = self.execute_query("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                                  (self.table,))
        table_exists = (rows[0][0] == 1)
        if table_exists:
            db_column_definitions = map(lambda row: dict(column_name=row[1],
                                                         column_type=row[2].upper(),
                                                         default_value=row[4]),
                                          self.execute_query("PRAGMA table_info(%s)" % (self.table,)))
            for db_column_definition in db_column_definitions:
                db_column_name = db_column_definition["column_name"]
                if db_column_name not in self.columns:
                    print "Column %s present in table but not in schema. We'll ignore it, so it won't be visible in the ORM!" \
                        % (db_column_name,)
                    continue
                db_column_type = db_column_definition["column_type"]
                orm_column_type = ORM.known_column_types[self.columns[db_column_name]["type"]]
                if db_column_type != orm_column_type:
                    raise TypeError("Column %s present in table has different type (%r) from the one in schema (%r)!" \
                                    % (db_column_name, db_column_type, orm_column_type))
                db_default_value = db_column_definition["default_value"]
                orm_default_value = self.columns[db_column_name].get("default", None)
                if db_default_value != orm_default_value:
                    raise TypeError("Column %s present in table has different default value (%r) from the one in schema (%r)" \
                                    % (db_column_name, db_default_value, orm_default_value))
            # If we got here, it means none of the table columns are
            # of a different type than what's in our schema.
            for orm_column_name, orm_column_definition in self.columns.items():
                if orm_column_name in map(lambda db_column_definition: db_column_definition["column_name"],
                                          db_column_definitions):
                    continue
                else:
                    orm_column_type = ORM.known_column_types[orm_column_definition["type"]]
                    orm_default_value = orm_column_definition.get("default", None)
                    query = "ALTER TABLE %s ADD COLUMN %s %s" % (self.table, orm_column_name, orm_column_type)
                    if orm_default_value is not None:
                        query += " DEFAULT %s" % (orm_default_value,)
                    self.execute_query(query)
        else:
            column_definition_chunks = []
            for orm_column_name, orm_column_definition in self.columns.items():
                if orm_column_name == ORMBase.id_key_name:
                    continue
                orm_column_type = ORM.known_column_types[orm_column_definition["type"]]
                orm_default_value = orm_column_definition.get("default", None)
                column_definition_chunks.append((orm_column_name,
                                                 orm_column_type,
                                                 orm_default_value))
            def column_definition_chunk_to_query_chunk((column_name, column_type, default_value)):
                query_chunk = "%s %s" % (column_name, column_type)
                if default_value is not None:
                    query_chunk += " DEFAULT %s" % (default_value,)
                return query_chunk
            column_definitions = ",".join(map(column_definition_chunk_to_query_chunk, column_definition_chunks))
            self.execute_query("CREATE TABLE %s (%s INTEGER PRIMARY KEY AUTOINCREMENT, %s)"
                            % (self.table, ORMBase.id_key_name, column_definitions))

    def get_all_objects(self):
        def row_to_object(row):
            kwargs = {}
            for column_name, row_value in zip(row.keys(), list(row)):
                kwargs[column_name] = row_value
            return self.mapped_object(**kwargs)
        rows = self.execute_query("SELECT %s FROM %s" % (",".join(self.columns), self.table))
        objects = map(row_to_object, rows)
        return objects

    def insert_row(self, mapped_object):
        column_names, values = zip(*[(column_name, mapped_object.__getattribute__(column_name))
                                     for column_name in self.columns if column_name in dir(mapped_object)])
        self.execute_query("INSERT INTO %s (%s) VALUES (%s)" % (self.table,
                                                                ",".join(column_names),
                                                                ",".join("?"*len(values))),
                           values)
        new_id = self.execute_query("SELECT %s FROM %s ORDER BY %s DESC LIMIT 1" % (ORMBase.id_key_name, self.table, ORMBase.id_key_name))[0][0]
        setattr(mapped_object, ORMBase.id_key_name, new_id)

    def update(self, mapped_object):
        column_setters = [(column_name, mapped_object.__getattribute__(column_name))
                          for column_name in self.columns if column_name in dir(mapped_object)]
        for column_name, new_value in column_setters:
            self.execute_query("UPDATE %s SET %s=? WHERE %s=?" % (self.table, column_name, ORMBase.id_key_name),
                               [new_value, getattr(mapped_object, ORMBase.id_key_name)])
