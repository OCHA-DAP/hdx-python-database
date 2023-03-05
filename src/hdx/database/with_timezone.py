from sqlalchemy.orm import DeclarativeBase, declared_attr


class Base(DeclarativeBase):
    @declared_attr.directive
    def __tablename__(cls):
        return f"{cls.__name__.lower()}s"
