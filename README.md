# TinyORM

TinyORM is a tiny [SQLite3](https://www.sqlite.org/) [ORM](https://en.wikipedia.org/wiki/Object-relational_mapping) for Python. It is designed to be very simple and unobtrusive to use. If you want to quickly read or write some data from a SQLite database without doing any initial setup or refactoring of code, this might be the library for you.

One of the biggest goals of this project is to have a truly tiny ORM that can be trivially dropped into an existing project. Accordingly, installation instructions are as simple as dropping the [`tiny_orm.py`](https://raw.githubusercontent.com/leonidg/tiny-orm/master/tiny_orm.py) file into your codebase. Just add `from tiny_orm import ORM` into your code and you're good to go.

**Please note that TinyORM is _not_ intended for production use. I am reasonably confident that it does not have SQL injection vulnerabilities, but it's not optimized for either speed or memory usage and it is _not_ well-tested. Use at your own risk.**

## Documentation

`tiny_orm` supplies just one user-facing function, called `ORM()`. It accepts two arguments:

1. The database (either a path to a file or "`:memory:`" (i.e. what is passed to `sqlite3.connect()`)
2. A configuration dictionary. The configuration dictionary should have a "`table`" entry for the table that is being mapped and a "`columns`" entry for the columns in that table.

Here is an example:

```python
Person = ORM("my-database.sqlite3", {
   "table": "people",
   "columns": {
       "first_name": str,
       "last_name": str,
       "age": int,
       "female": bool
   }
})
```

The value of the columns in this example are their types. The types that TinyORM recognizes are `str`, `int`, `bool`, and `float`.

Once `Person` is defined, you can use it:

```python
>>> alice = Person(first_name="Alice", last_name="Alison", age=50, female=True)
>>> bob = Person(first_name="Bob", last_name="Bobward", female=False)
>>> alice.first_name
Alice
>>> Alice.female
True
>>> Bob.age # returns None, since not supplied
>>> alice.save() # Alice is now added to the "people" table
>>> bob.save() # Same with Bob
>>> bob.ag = 90 # Typo! We meant "age" here
TypeError
>>> bob.age = 67
>>> bob.save() # Bob's row in the "people" table is updated to show his age as 67
```

As you can see, the `Person` object acts like a class that can be instantiated to create objects, which are mapped to rows in our table. The columns we initially supplied to `ORM` are exactly the parameters that this object accepts; any that we do not supply are set to `None` and no additional ones are allowed. This helps prevent typos.

### Default values

You can supply default values for columns:

```python
DoeFamilyMember = ORM("my-database.sqlite3", {
    "table": "people",
    "columns": {
        "first_name": str,
        "last_name": { "type": str, "default": "Doe" },
        "age": int,
        "female": bool
    }
})
```

`DoeFamilyMember` works just like `Person` except that if you do not supply a last name, it will automatically get set to `Doe` instead of `None`. To support this syntax, TinyORM uses the dictionary configuration syntax for the columns. The shorter type-only syntax used in the other columns is actually just short-form; you could use the longer syntax for all the columns. This is identical to the previous example:

```python
DoeFamilyMember = ORM("my-database.sqlite3", {
    "table": "people",
    "columns": {
        "first_name": { "type": str },
        "last_name": { "type": str, "default": "Doe" },
        "age": { "type": int },
        "female": { "type": bool }
    }
})
```

### Helper functions

The values returned by `ORM` are not just callables for creating new objects. They also have methods of their own. Currently, the only implemented method is `get_all_objects()`. Continuing from the `alice`/`bob` example above:

```python
>>> people = Person.get_all_objects()
>>> people[0].first_name
Alice
>>> people[1].last_name
Bob
>>>
```

## TODO

* Implement a `delete()` method on objects (just like `save()`).
* Allow for an easy way to consume an existing database.

## Acknowledgements

Much of this code was written for work I needed to do at the [New York Times](http://developers.nytimes.com). Republished here with permission.
