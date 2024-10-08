#!/usr/bin/env python
#
# Copyright 2010 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Operations which can be yielded from mappers.

Operation is callable that takes context.Context as a parameter.
Operations are called during mapper execution immediately
on recieving from handler function.
"""



# These are all relative imports.
from . import db
from . import counters
from .base import Operation

__all__ = ['db', 'counters', 'Operation']
