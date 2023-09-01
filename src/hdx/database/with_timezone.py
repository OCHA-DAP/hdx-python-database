from sqlalchemy.orm import DeclarativeBase, declared_attr

from .utils import camel_to_snake_case


class Base(DeclarativeBase):
    @declared_attr.directive
    def __tablename__(cls):
        return camel_to_snake_case(cls.__name__)
