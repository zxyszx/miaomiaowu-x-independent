"""Read-only, fail-closed aggregate Grant execution status for administrators."""
import json
import math
import sqlite3

from grant_ack import fact_for
from grant_execution import GrantExecution
from grant_lifecycle import STATES
from node_config import digest


TABLES = frozenset((
    'usage_first_total_grants',
    'usage_first_total_configurations',
    'usage_first_total_installations',
    'usage_first_total_applied',
    'usage_first_total_lifecycle',
    'usage_first_total_execution',
))


def _row(db, available, table, grant_id):
    if table not in available:
        return None
    return db.execute(
        f'SELECT * FROM {table} WHERE grant_id=?', (grant_id,)).fetchone()


def _json_object(value):
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    return parsed if type(parsed) is dict else None


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      allow_nan=False)


def _configuration(row, grant):
    if row is None:
        return None
    value = _json_object(row['payload'])
    if value is None:
        return False
    try:
        encoded = _canonical(value)
        valid = (
            encoded == row['payload']
            and row['digest'] == digest(value)
            and value['schema'] == 'independent.total-grant.binding.v1'
            and value['grant_id'] == grant['grant_id']
            and value['server_id'] == grant['server_id']
            and value['session_id'] == grant['session_id']
            and value['cycle_id'] == grant['cycle_id']
            and value['policy_id'] == grant['policy_id']
            and value['node_id'] == grant['node_id']
            and value['client']['user_id'] == grant['user_id']
            and value['client']['uuid'] == grant['credential']
            and value['reserved_bytes'] == grant['reserved_bytes']
            and value['baseline_bytes'] == int(grant['baseline_bytes'])
            and value['issued_at'] == grant['issued_at']
            and grant['issued_at'] < value['valid_until'] <= grant['valid_until']
        )
    except (KeyError, TypeError, ValueError, OverflowError):
        return False
    return dict(value, digest=row['digest']) if valid else False


def _record(db, available, grant, agent, online, now):
    grant_id = grant['grant_id']
    configuration_row = _row(
        db, available, 'usage_first_total_configurations', grant_id)
    installation_row = _row(
        db, available, 'usage_first_total_installations', grant_id)
    applied = _row(db, available, 'usage_first_total_applied', grant_id)
    lifecycle = _row(db, available, 'usage_first_total_lifecycle', grant_id)
    execution_row = _row(
        db, available, 'usage_first_total_execution', grant_id)

    missing = any(row is None for row in (
        configuration_row, installation_row, applied, lifecycle, execution_row))
    invalid = False
    configuration = _configuration(configuration_row, grant)
    if configuration is False:
        invalid = True

    installation = (_json_object(installation_row['fact'])
                    if installation_row is not None else None)
    execution = (_json_object(execution_row['fact'])
                 if execution_row is not None else None)
    if ((installation_row is not None and installation is None)
            or (execution_row is not None and execution is None)):
        invalid = True
    lifecycle_state = lifecycle['state'] if lifecycle is not None else None
    if lifecycle_state not in STATES and lifecycle_state is not None:
        lifecycle_state = 'invalid'
        invalid = True

    if configuration not in (None, False):
        try:
            expected_installation = fact_for(configuration)
            if (installation is not None
                    and _canonical(installation) != _canonical(expected_installation)):
                invalid = True
            if applied is not None and (
                    applied['config_version'], applied['config_digest'],
                    applied['valid_until']) != (
                        expected_installation['revision'],
                        expected_installation['digest'],
                        expected_installation['valid_until']):
                invalid = True
            if lifecycle is not None and (
                    lifecycle['server_id'], lifecycle['session_id'],
                    lifecycle['configuration_digest']) != (
                        grant['server_id'], grant['session_id'],
                        configuration['digest']):
                invalid = True
        except (KeyError, TypeError, ValueError, OverflowError):
            invalid = True
    elif installation is not None or applied is not None or lifecycle is not None:
        invalid = True

    if execution is not None:
        try:
            GrantExecution._validate_fact(execution)
            expected_execution = dict(installation or {},
                                      execution_id=execution['execution_id'])
            if (_canonical(execution) != _canonical(expected_execution)
                    or execution['grant_id'] != grant_id
                    or execution['valid_until'] > grant['valid_until']
                    or type(execution_row['recorded_at']) not in (int, float)
                    or isinstance(execution_row['recorded_at'], bool)
                    or not math.isfinite(execution_row['recorded_at'])
                    or not grant['issued_at'] <= execution_row['recorded_at'] <= now):
                invalid = True
        except (KeyError, TypeError, ValueError, OverflowError):
            invalid = True

    identity_current = bool(
        agent is not None
        and grant['server_id'] == agent['server_id']
        and grant['session_id'] == agent['session_id']
        and grant['token_hash'] == agent['token_hash'])
    # A binding may deliberately end before its outer Grant reservation.  The
    # exact installation/applied/execution checks above all bind this value;
    # never extend current execution back to the reservation's upper bound.
    execution_until = (configuration['valid_until']
                       if configuration not in (None, False)
                       else grant['valid_until'])
    expired = not (grant['issued_at'] <= now < execution_until)
    complete = not missing and not invalid
    current = bool(
        online and identity_current and complete and not expired
        and lifecycle_state == 'installed')
    if invalid:
        integrity = 'invalid'
    elif missing:
        integrity = 'incomplete'
    elif not identity_current:
        integrity = 'stale_identity'
    else:
        integrity = 'verified'
    return {
        'lifecycle': lifecycle_state,
        'execution_recorded': execution_row is not None,
        'integrity': integrity,
        'expired': expired,
        'current': current,
        'ready_for_execution': False,
    }


def _invalid_record(db, available, grant_id):
    lifecycle_state = 'invalid'
    execution_recorded = False
    try:
        lifecycle = _row(
            db, available, 'usage_first_total_lifecycle', grant_id)
        if lifecycle is None:
            lifecycle_state = None
        elif lifecycle['state'] in STATES:
            lifecycle_state = lifecycle['state']
        execution_recorded = _row(
            db, available, 'usage_first_total_execution', grant_id) is not None
    except (KeyError, TypeError, sqlite3.Error):
        pass
    return {
        'lifecycle': lifecycle_state,
        'execution_recorded': execution_recorded,
        'integrity': 'invalid',
        'expired': True,
        'current': False,
        'ready_for_execution': False,
    }


def aggregate(db, server_id, agent, online, now):
    """Return only non-secret status derived from existing durable evidence."""
    available = frozenset(row['name'] for row in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"))
    if 'usage_first_total_grants' not in available:
        return {
            'evidence_status': 'unavailable',
            'current': False,
            'current_count': 0,
            'execution_recorded_count': 0,
            'records': [],
            'ready_for_execution': False,
        }
    try:
        grants = list(db.execute(
            'SELECT * FROM usage_first_total_grants WHERE server_id=? '
            'ORDER BY issued_at,grant_id', (server_id,)))
    except sqlite3.Error:
        return {
            'evidence_status': 'unavailable',
            'current': False,
            'current_count': 0,
            'execution_recorded_count': 0,
            'records': [],
            'ready_for_execution': False,
        }
    records = []
    for grant in grants:
        try:
            records.append(_record(db, available, grant, agent, online, now))
        except (KeyError, TypeError, ValueError, OverflowError, sqlite3.Error):
            grant_id = grant['grant_id'] if 'grant_id' in grant.keys() else None
            records.append(_invalid_record(db, available, grant_id))
    evidence_status = ('available' if TABLES <= available else 'partial')
    return {
        'evidence_status': evidence_status,
        'current': any(record['current'] for record in records),
        'current_count': sum(record['current'] for record in records),
        'execution_recorded_count': sum(
            record['execution_recorded'] for record in records),
        'records': records,
        'ready_for_execution': False,
    }
