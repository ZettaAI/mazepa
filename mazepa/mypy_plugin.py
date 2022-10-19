# pylint: disable=all # type: ignore
from __future__ import annotations
from typing import Callable, Final, Optional
from mypy.plugin import Plugin, ClassDefContext
from mypy.types import Parameters

from mypy.plugins.common import (
    # add_attribute_to_class,
    add_method_to_class,
)

from mypy.types import (
    AnyType,
    TypeOfAny,
)


def task_maker_cls_callback(ctx):  # pragma: no cover # type: ignore
    call_method = ctx.cls.info.get_method("__call__")
    if call_method is not None and call_method.type is not None:
        args = call_method.arguments[1:]
        for arg in args:
            if arg.type_annotation is None:
                arg.type_annotation = AnyType(TypeOfAny.unannotated)

        arg_types = [arg.type_annotation for arg in args]
        arg_kinds = [arg.kind for arg in args]
        # skip `self`
        arg_names = call_method.arg_names[1:]
        params = Parameters(arg_types=arg_types, arg_names=arg_names, arg_kinds=arg_kinds)
        arg_names = []
        return_type = ctx.api.named_type(
            fullname="mazepa.Task",
            args=[
                # AnyType(TypeOfAny.unannotated),
                params,
                call_method.type.ret_type,
            ],
        )
        add_method_to_class(
            ctx.api,
            ctx.cls,
            "make_task",
            args=args,
            return_type=return_type,
        )

    return True


def flow_type_cls_callback(ctx):  # pragma: no cover # type: ignore
    generate_method = ctx.cls.info.get_method("generate")

    if generate_method is not None:
        args = generate_method.arguments[1:]  # skip `self`
        for arg in args:
            if arg.type_annotation is None:
                arg.type_annotation = AnyType(TypeOfAny.unannotated)

        arg_types = [arg.type_annotation for arg in args]
        arg_kinds = [arg.kind for arg in args]
        arg_names = generate_method.arg_names[1:]  # skip `self`
        params = Parameters(arg_types=arg_types, arg_names=arg_names, arg_kinds=arg_kinds)

        return_type = ctx.api.named_type(
            fullname="mazepa.Flow",
            args=[
                # AnyType(TypeOfAny.unannotated),
                params,
            ],
        )
        add_method_to_class(
            ctx.api,
            ctx.cls,
            "__call__",
            args=args,
            return_type=return_type,
        )

    return True


TASK_FACTORY_CLS_MAKERS: Final = {"mazepa.tasks.task_factory_cls"}
FLOW_TYPE_CLS_MAKERS: Final = {"mazepa.flows.flow_type_cls"}


class MazepaPlugin(Plugin):
    def get_class_decorator_hook_2(
        self, fullname: str
    ) -> Optional[Callable[[ClassDefContext], bool]]:  # pragma: no cover
        if fullname in TASK_FACTORY_CLS_MAKERS:
            return task_maker_cls_callback
        if fullname in FLOW_TYPE_CLS_MAKERS:
            return flow_type_cls_callback
        return None


class DummyPlugin(Plugin):  # pragma: no cover
    pass


def plugin(version):  # pragma: no cover
    try:
        import mazepa  # pylint: disable
    except ModuleNotFoundError:
        return DummyPlugin
    else:
        return MazepaPlugin
