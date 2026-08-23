import assert from 'node:assert/strict';
import test from 'node:test';
import { echo } from '../src/echo.js';

test('echo preserves argument order', () => assert.equal(echo(['one', 'two']), 'one two'));
