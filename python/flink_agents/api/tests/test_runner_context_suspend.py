################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
#################################################################################
"""Tests for mailbox-yielding action suspend."""

import time

from flink_agents.api.runner_context import RunnerContext


def test_suspend_yields_before_deadline() -> None:
    """The first step of suspend returns immediately instead of sleeping."""
    coroutine = RunnerContext.suspend(None, 0.5)
    start = time.monotonic()
    coroutine.send(None)
    elapsed = time.monotonic() - start
    coroutine.close()
    assert elapsed < 0.1
