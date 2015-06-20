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


class ORMBase(object):
    """
    This class serves as the base for each instance of a "mapped
    object," which is to say an object representing a row in a SQLite
    table.
    """
    def __init__(self, orm):
        self.__orm = orm
    def save(self):
        if not hasattr(self, "id"):
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
        if not set(schema["columns"].values()).issubset(set(ORM.known_column_types.keys())):
            raise TypeError("Illegal column type! Must be one of %s." % (ORM.known_column_types.keys(),))
        if "id" in schema["columns"].keys():
            raise TypeError("Cannot use 'id' as column name. It's reserved.")
        self.connection = sqlite3.connect(database_name, check_same_thread=False)
        self.connection.row_factory = sqlite3.Row
        self.connection_lock = threading.RLock()
        self.table = schema["table"]
        self.columns = copy.deepcopy(schema["columns"])
        self.columns["id"] = int
        self.sync() # Will raise TypeError if columns unsafe
        self.mapped_object = ClassFactory("%s_Mapper" % (self.table,),
                                          argument_typecasters=self.columns,
                                          exceptions=["_ORMBase__orm"], # This is a magic internal name of the
                                                                        # __orm object that BaseClass defines.
                                          BaseClass=ORMBase,
                                          orm=self)

    def __call__(self, **kwargs):
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
            - If table exists and contains a column with the same name but different type as
              one of the given ones, raises an exception.
        """
        rows = self.execute_query("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?",
                                  (self.table,))
        table_exists = (rows[0][0] == 1)
        if table_exists:
            column_definition_pairs = map(lambda row: (row[1], row[2].upper()),
                                          self.execute_query("PRAGMA table_info(%s)" % (self.table,)))
            for (column_name, column_type) in column_definition_pairs:
                if column_name not in self.columns:
                    print "Column %s present in table but not in schema. We'll ignore it, so it won't be visible in the ORM!" % (column_name,)
                    continue
                if column_type != ORM.known_column_types[self.columns[column_name]]:
                    raise TypeError("Column %s present in table has different type from the one in schema!" % (column_name,))
            # If we got here, it means none of the table columns are
            # of a different type than what's in our schema.
            for column_name, column_type in self.columns.items():
                if column_name in map(lambda (column_name, column_type): column_name, column_definition_pairs):
                    continue
                else:
                    self.execute_query("ALTER TABLE %s ADD COLUMN %s %s"
                                       % (self.table, column_name, ORM.known_column_types[column_type]))
        else:
            column_definition_pairs = [(column_name, ORM.known_column_types[column_type])
                                       for (column_name, column_type) in self.columns.items()
                                       if column_name != "id"]
            column_definitions = ",".join(map(lambda (column_name, column_type): "%s %s" % (column_name, column_type), column_definition_pairs))
            self.execute_query("CREATE TABLE %s (id INTEGER PRIMARY KEY AUTOINCREMENT, %s)"
                            % (self.table, column_definitions))

    def get_all_objects(self):
        def row_to_object(row):
            kwargs = {}
            for column_name, row_value in zip(row.keys(), list(row)):
                if row_value is not None:
                    kwargs[column_name] = row_value
            return self.mapped_object(**kwargs)
        rows = self.execute_query("SELECT %s FROM %s" % (",".join(self.columns), self.table,))
        objects = map(row_to_object, rows)
        return objects

    def insert_row(self, mapped_object):
        column_names, values = zip(*[(column_name, mapped_object.__getattribute__(column_name))
                                     for column_name in self.columns if column_name in dir(mapped_object)])
        self.execute_query("INSERT INTO %s (%s) VALUES (%s)"
                           % (self.table,
                              ",".join(column_names),
                              ",".join(["?" for value in values])),
                           values)
        new_id = self.execute_query("SELECT id FROM %s ORDER BY id DESC LIMIT 1" % (self.table,))[0][0]
        mapped_object.id = new_id

    def update(self, mapped_object):
        column_setters = [(column_name, mapped_object.__getattribute__(column_name))
                        for column_name in self.columns if column_name in dir(mapped_object)]
        for column_name, new_value in column_setters:
            self.execute_query("UPDATE %s SET %s=? WHERE id=?" % (self.table, column_name),
                               [new_value, mapped_object.id])
