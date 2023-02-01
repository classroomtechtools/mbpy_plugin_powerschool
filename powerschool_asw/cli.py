import pdb
import click
from uplink import (
    Consumer,
    RequestsClient,
    Body,
    Path,
    Query,
    post,
    returns,
    headers
)
from types import SimpleNamespace
import re
import datetime
from collections import defaultdict
from mbpy_endpoints.endpoints import Endpoint
from json.decoder import JSONDecodeError
from src.mbpy.db.utils import cache_install
import pandas as pd
import flatdict
import os


def dot(data):
    if type(data) is list:
        return list(map(dot, data))
    elif type(data) is dict:
        sns = SimpleNamespace()
        for key, value in data.items():
            setattr(sns, key, dot(value))
        return sns
    else:
        return data


def get_dotted_path(data: dict, path: str, default=None):
    pathList = re.split(r'\.', path, flags=re.IGNORECASE)
    result = data
    for key in pathList:
        try:
            result = result[key]
        except:
            result = default
            break

    return result

from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

import io, ssl

def export_csv(df):
  with io.StringIO() as buffer:
    df.to_csv(buffer, index=False)
    return buffer.getvalue()

def send_email(from_, send_to, subject, body, password, *dataframes):
    multipart = MIMEMultipart()

    multipart['From'] = from_
    multipart['To'] = ','.join(send_to)
    multipart['Subject'] = subject
    
    for filename, df in dataframes:
        attachment = MIMEApplication(export_csv(df), Name=filename)
        attachment['Content-Disposition'] = f'attachment; filename="{filename}"'
        multipart.attach(attachment)

    multipart.add_header('Content-Type','text/plain')
    multipart.attach(MIMEText(body, 'plain'))

    context = ssl.create_default_context()
    data = multipart.as_bytes()
    with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as email:
        email.login(from_, password)
        email.sendmail(from_, send_to, data)


@headers({'Content-Type': 'application/x-www-form-urlencoded'})
class GetToken(Consumer):
    def __init__(self, client_id, client_secret, client=RequestsClient):
        base_url = 'https://psweb.asw.waw.pl/'
        super(GetToken, self).__init__(base_url=base_url, client=client)
        bearer_token = base64.b64encode(bytes(client_id + ":" + client_secret, "ISO-8859-1")).decode("ascii")
        self.session.headers["Authorization"] = f'Basic {bearer_token}'

    @post('oauth/access_token')
    @returns.json(key='access_token')
    def get_access_token(self, grant_type: Query = 'client_credentials'):
        pass


import base64

@headers({'Content-Type': 'application/json'})
class PsWeb(Consumer):
    def __init__(self, client_id, client_secret, client=RequestsClient):
        auth = GetToken(client_id, client_secret)
        response = auth.get_access_token().json()
        access_token = response.get('access_token')
        if access_token is None:
            raise Exception("No access token returned!")

        base_url = 'https://psweb.asw.waw.pl/ws/schema/query/'
        super(PsWeb, self).__init__(base_url=base_url, client=client)
        self.session.headers["Authorization"] = f'Bearer {access_token}'

    @post('mk.ManageBac_Stu')
    def get_students(self, pagesize: Query = 2000, page: Query = 1, **body: Body):
        pass

    @post('mk.ManageBac_Par')
    def get_parents(self, pagesize: Query = 2000, page: Query = 1, **body: Body):
        pass

    @post('mk.ManageBac_Tea')
    def get_teachers(self, pagesize: Query = 2000, page: Query = 1, **body: Body):
        pass

    @post('mk.ManageBac_Stu_Class')
    def get_enrollments(self, pagesize: Query = 10000, page: Query = 1, **body: Body):
        pass


def load_entity(api, entity, path):
    method = getattr(api, f'get_{entity}')
    response = method()
    if not response.ok:
        raise Exception(f'{response.request.url} => {response.status_code}\n{response.text}')
    try:
        json = response.json()
    except JSONDecodeError:
        # something wrong with the endpoint, should probably fail out in production
        json = {}
    has_records = json.get('record', False)
    if not has_records:
        message = f'No {entity} records found? Must be an issue with the powerschool source. Exiting with no actions taken'
        raise Exception(message)
           
    objects = {}
    records = json.get('record', [])
    flattened_records = [dict(flatdict.FlatDict(
        item, delimiter='.')) for item in records]
    df = pd.DataFrame.from_records(flattened_records)
    df.to_csv(f'/tmp/output_{entity}.csv', index=False)
    for item in records:
        value = get_dotted_path(item, path)
        objects[value] = dot(item)
    return (df, objects)


def load_enrollments(api):
    """
    """
    page = 1
    objects = defaultdict(lambda: defaultdict(dict))
    classes = []
    records = []
    mapped_classes = []
    while True:
        response = api.get_enrollments(page=page)
        json = response.json()
        these_records = json.get('record', [])
        records.extend(these_records)
        for item in these_records:
            dotted = dot(item)
            section_number = dotted.tables.sections.section_number
            class_id = dotted.tables.sections.class_id
            if not section_number.isdigit():
                class_id = f'{class_id}{section_number}'
                mapped_classes.append(class_id)
            classes.append(class_id)
            objects[dotted.tables.students.student_number][class_id] = dotted
        page += 1

        if json.get('record') is None:
            break
        
    df = pd.DataFrame.from_records(records)
    df.to_csv(f'/tmp/output_schedule.csv', index=False)
    #df = pd.DataFrame.from_records([{'uniq_id': clss} for clss in set(mapped_classes)])
    #df.to_csv(f'/tmp/output_mapped_classes.csv', index=False)
    return (df, objects, set(classes))


def get_entity_by_key(mb: Endpoint, entity: str, key: str, query: str):
    """    
    """
    method = getattr(mb.endpoints, f'get_{entity}')
    results = [item for item in method(q=query).get(entity) if item.get(key) == query.strip()]
    if len(results) == 0:
        return None
    assert len(results) == 1, 'Issue with multiple students with same `student_id`'
    return results.pop()


def execute(mb: Endpoint, records, description, *args, **kwargs):
    """
    Interact with the endpoint, add changed record
    """
    response = None
    try:
        response = mb(*args, **kwargs)
        record = {
            'description': description,
            'action': mb.__name__,
            'change': True,
            'error': bool(response.get('error')),
            'response': response,
            'body': kwargs.get('body')
        }

    except Exception as err:
        record = {
            'description': description,
            'action': mb.__name__,
            'change': False,
            'error': True,
            'response': str(err),
            'body': 'Unexpected Error'
        }

    record.update(kwargs)
    records.append(record)
    return response


@click.command('sync-asw')
@click.option('-d', '--date', 'date', type=click.DateTime(formats=['%Y-%m-%d']), default=str(datetime.date.today()), help='The date on which to execute')
@click.option('-x', '--postfix', 'postfix', default='')
@click.option('-a', '--associate/--skip-associations', 'associations', is_flag=True, default=False)
@click.option('-p', '--skip-profile/--update-profile', 'profiles', is_flag=True, default=False)
@click.option('-u', '--user', 'user', default=os.environ.get('USER'))
@click.option('-w', '--password', 'password', default=os.environ.get('PASSWORD'))
@click.option('-t', '--to', 'to_whom', default=[], multiple=True)
@click.pass_obj
def sync(obj, date, postfix, associations, profiles, user, password, to_whom):
    """
    Specialist software written for American School Warsaw
    """
    # init mb and ps apis
    #cache_install('asw_122342')
    
    if len(to_whom) > 0 and not password:
        raise Exception('Please provide password to send email')

    from mbpy_endpoints import Generator
    date_string = date.strftime('%Y-%m-%d')

    # if not dry_run:
    #     #raise Exception('You must remove me before going into production, sir!')
    #     mb = Generator(auth_token=obj.token, tld=obj.tld, subdomain=obj.baseurl_subdomain, verbosity=1)
    # else:
    #     mb = Generator(auth_token=obj.token, tld=obj.tld, subdomain=obj.baseurl_subdomain, verbosity=1, Endpoint_class=MockEndpoint)
    #fake_mb = Generator(auth_token=obj.token, tld=obj.tld, subdomain=obj.baseurl_subdomain, verbosity=1, Endpoint_class=MockEndpoint)

    mb = obj.Generator

    api = PsWeb(
        client_id='2f33e0f2-6274-4887-9925-2b678fbdbc6a',
        client_secret='53d88337-bc62-410b-91cf-d732558a8da9'
    )

    psdf_enrollments, ps_student_enrollments, _ = load_enrollments(api)
    mb_student_enrollments = defaultdict(list)
    psdf_students, ps_students = load_entity(
        api, 'students', 'tables.students.student_number')
    
    psdf_teachers, teachers = load_entity(api, 'teachers', 'tables.teachers.id')
    psdf_parents, parents = load_entity(api, 'parents', 'tables.students.student_number')

    to_be_removed = defaultdict(lambda: defaultdict(dict))
    fields_to_be_updated = defaultdict(lambda: defaultdict(dict))

    records = []
    missing_classes = []

    try:
        mb_year_groups = {}
        for year_group in mb.generate_year_groups():
            mb_year_groups[year_group.get('grade')] = year_group

        print('Creating accounts')
        mb_students = {}
        for student in mb.generate_students():
            student_id = student.get('student_id')
            mb_students[student_id] = student

        for stu_id, ps_student in ps_students.items():
            mb_student = mb_students.get(stu_id)
            if not mb_student is None and mb_student.get('archived'):
                execute(mb.endpoints.unarchive_a_student, records, stu_id, id=mb_student.get('id'))
            if mb_student is None:
                day, month, year = ps_student.tables.students.dateofbirth.split('-')
                class_grade_number = int(ps_student.tables.students.grade.split(' ')[1]) + 1
                body = {
                    'student': {
                        'student_id': stu_id,
                        'birthday': f'{year}-{month}-{day}',
                        'middle_name': ps_student.tables.students.middle_name,
                        'last_name': ps_student.tables.students.last_name,
                        'first_name': ps_student.tables.students.first_name,
                        'email': ps_student.tables.students.email,
                        'nickname': ps_student.tables.u_student_additionals.nickname,
                        'gender': {'F': 'Female', 'M': 'Male'}.get(ps_student.tables.students.gender),
                        'nationalities': ps_student.tables.u_country_codes.nat,
                        'class_grade_number': class_grade_number
                    }
                }
                new_student = execute(mb.endpoints.create_student, records, stu_id, body=body)
                if new_student is None:
                    records.append({
                        'description': 'Error occurred when trying to create student',
                        'action': 'Create student',
                        'error': True,
                        'change': False,
                        'response': None,
                        'body': ''
                    })

                else:
                    record = new_student.get('student', new_student)  # convoluted for dev


                    # FIXME: add them to the right year group
                    target_year_group = mb_year_groups.get(ps_student.tables.students.grade)
                    assert target_year_group is not None, 'Grade is wrong?'
                    if not record.get('id') in target_year_group.get('student_ids'):
                        execute(mb.endpoints.add_to_year_group, records, f'{record.get("student_id")} > {ps_student.tables.students.grade}', id=target_year_group.get('id'), body={
                            'student_ids': [record.get('id')]
                        })


                    if not 'id' in record:
                        print(f'Not adding student {stu_id}')
                        continue  # dev
                    mb_students[stu_id] = record

        mb_teachers = {}
        for teacher in mb.generate_teachers():
            email = teacher.get('email').lower()
            mb_teachers[email] = teacher

        for email, ps_teacher in teachers.items():
            mb_teach = mb_teachers.get(email)
            if mb_teach is None:
                body = {
                    'teacher': {
                        'email': ps_teacher.tables.teachers.id,
                        'first_name': ps_teacher.tables.teachers.first_name,
                        'last_name': ps_teacher.tables.teachers.last_name,
                        'middle_name': ps_teacher.tables.teachers.middle_name
                    }
                }
                new_teacher = execute(mb.endpoints.create_teacher, records, email, body=body)
                if not 'id' in new_teacher:
                    if error := new_teacher.get('errors'):
                        print(error)
                    print(f'Not adding teacher {email}')
                    continue  # dev
                mb_teachers[email] = new_teacher


        mb_parents = {}
        for parent in mb.generate_parents():
            email = parent.get('email').lower()
            mb_parents[email] = parent

        for stu_id, ps_parent in parents.items():
            mb_stu = mb_students.get(stu_id)  # get_entity_by_key(mb, 'students', 'student_id', stu_id)
            if mb_stu is None:
                continue  # can occur in dev environment

            # link parents to students
            base = ps_parent.tables.u_student_additionals
            parent_list = []
            for par in ['mother', 'father']:
                kind = par.title()
                email = getattr(base, f'{par}_school_email')
                if email is None:
                    records.append({
                        'description': '',
                        'action': 'missing_email',
                        'error': False,
                        'change': False,
                        'response': f'{stu_id} has no parent email for {kind}',
                        'body': ''
                    })
                    continue
                split = email.split('@')[0].split('_')
                first_name = f'{par}_first_name'
                first_name = getattr(base, first_name) if hasattr(base, first_name) else split[1]
                last_name = f'{par}_last_name'
                last_name = getattr(base, last_name) if hasattr(base, last_name) else split[0]
                parent_list.append((kind, {
                    'email': email,
                    'first_name': first_name.title(),
                    'last_name': last_name.title(),
                    'gender': {'mother': 'Female', 'father': 'Male'}.get(par)
                }))

            for role, parent in parent_list:
                email = parent.get('email')
                mb_parent = mb_parents.get(email)  # get_entity_by_key(mb, 'parents', 'email', email)
                if mb_parent is None:
                    new_parent = execute(mb.endpoints.create_parent, records, email, body={
                        'parent': parent
                    })
                    if not 'id' in new_parent:
                        continue  # dev, will not be able to associate

                    # associate it here immediately, so we don't have to rely on running --associations
                    execute(mb.endpoints.add_child_association, records, f'associate', parent_id=mb_parent.get('id'), body={
                        'child': {
                            'id': mb_stu.get('id'),
                            'relationship': role
                        }
                    })
                    mb_parents[email] = new_parent

                if mb_parent.get('archived'):
                    # shouldn't really get to this point, though, since we are unarchiving students, we'll get this for free
                    # although if it happens just above, we won't have latest info
                    # anyway, at least we'll have a record of it happening this way
                    execute(mb.endpoints.unarchive_a_parent, records, email, id=mb_parent.get('id'))

                if associations:
                    # off by default as it takes a very long time to execute, and rarely will change
                    relationships = list(mb.generate_parentchild_relationships(mb_parent.get('id')))
                    this_relationship = [rel.get('relationship') for rel in relationships if rel.get('id') == mb_stu.get('id')]
                    if len(this_relationship) == 0:
                        execute(mb.endpoints.add_child_association, records, f'{mb_stu.get("student_id")} -> {email}', parent_id=mb_parent.get('id'), body={
                            'child': {
                                'id': mb_stu.get('id'),
                                'relationship': role
                            }
                        })
                    elif this_relationship.pop() != role:
                        execute(mb.endpoints.update_child, records, email, parent_id=mb_parent.get('id'), child_id=mb_stu.get('id'), body={'child': {'relationship': role}})

        # uses generate memberships endpoint
        mb_classes = {}
        for clss in mb.generate_classes():
            uniq_id = clss.get('uniq_id')
            clss['archived'] = False
            mb_classes[uniq_id] = clss
        for clss in mb.generate_classes(archived=True):
            uniq_id = clss.get('uniq_id')
            assert uniq_id not in clss, 'Class uniq IDs are not unique'
            clss['archived'] = True
            mb_classes[uniq_id] = clss

        for memb in mb.generate_memberships(class_happens_on=date_string, classes='active', users='active', per_page=200):

            membership = dot(memb)
            clss = mb_classes.get(membership.uniq_class_id)  # get_entity_by_key(mb, 'classes', 'uniq_id', membership.class_id)

            if membership.role == 'Student':
                uniq_student_id = membership.uniq_student_id.strip()
                uniq_class_id = membership.uniq_class_id.strip()

                # store mb enrollments for later comparison
                mb_student_enrollments[uniq_student_id].append(uniq_class_id)

                if uniq_student_id != membership.uniq_student_id:
                    print(f'Whitespace "{uniq_student_id}"')
                if uniq_class_id != membership.uniq_class_id:
                    print(f'Whitespace "{uniq_class_id}"')
                mb_student = mb_students.get(membership.uniq_student_id)  # session.get(Student, membership.user_id)
                clss = mb_classes.get(membership.uniq_class_id)

                if enrolled := ps_student_enrollments[uniq_student_id][uniq_class_id]:
                    pass  # print(enrolled)
                else:
                    to_be_removed[uniq_student_id][uniq_class_id] = SimpleNamespace(
                        student=mb_student, clss=clss
                    )

                if ps_stu := ps_students.get(uniq_student_id):
                # ensure enrolled into correct year_group
                    target_year_group = mb_year_groups.get(ps_stu.tables.students.grade)
                    assert target_year_group is not None, 'Grade is wrong?'
                    if not mb_student.get('id') in target_year_group.get('student_ids'):
                        execute(mb.endpoints.add_to_year_group, records, f'{mb_student.get("student_id")} > {ps_stu.tables.students.grade}', id=target_year_group.get('id'), body={
                            'student_ids': [mb_student.get('id')]
                        })

                    if not profiles:
                        # stop here unless we need to update profiles
                        continue

                    if ps_stu.tables.students.grade != mb_student.get('class_grade'):
                        # FIXME: This doesn't seem to be working
                        execute(mb.endpoints.update_a_student, records, mb_student.get('student_id'), id=mb_student.get('id'), body={
                            'student': {
                                'class_grade_number': int(ps_stu.tables.students.grade.split(' ')[1]) + 1
                            }
                        })
                    # ps_birthday = datetime.datetime.strptime(
                    #     ps_stu.tables.u_student_additionals.enrollmentdate, '%d-%m-%Y').date()
                    dates_checks = (
                        #('attendance_start_date', mb_student.attendance_start_date, ps_stu.tables.u_student_additionals.enrollmentdate),
                        ('birthday', mb_student.get('birthday'),
                            ps_stu.tables.students.dateofbirth),
                    )
                    for property, mb_, ps_ in dates_checks:
                        if not ps_ is None:
                            ps_date = datetime.datetime.strptime(
                                ps_, '%d-%m-%Y').date()
                            if not mb_ is None:
                                mb_date = datetime.datetime.strptime(
                                    mb_, '%Y-%m-%d').date()
                                if ps_date != mb_date:
                                    # incorrect
                                    fields_to_be_updated[uniq_student_id][property] = ps_date.isoformat()
                            else:
                                # blank
                                fields_to_be_updated[uniq_student_id][property] = ps_date.isoformat()
                        else:
                            if not mb_ is None:
                                fields_to_be_updated[uniq_student_id][property] = None

                    field_checks = (
                        ('email', mb_student.get('email'), ps_stu.tables.students.email),
                        ('last_name', mb_student.get('last_name'),
                            ps_stu.tables.students.last_name),
                        ('first_name', mb_student.get('first_name'),
                            ps_stu.tables.students.first_name),
                        ('middle_name', mb_student.get('middle_name'),
                            ps_stu.tables.students.middle_name),
                        ('nickname', mb_student.get('nickname'),
                            ps_stu.tables.u_student_additionals.nickname),
                        ('class_grade', mb_student.get('class_grade'),
                            ps_stu.tables.students.grade),
                        #('nationalities', (mb_student.get('nationalities') or [None]).pop(),
                        #    ps_stu.tables.u_country_codes.nat),
                        ('gender', mb_student.get('gender'), {
                            'M': 'Male', 'F': 'Female'}.get(ps_stu.tables.students.gender))
                    )
                    
                    for property, mb_value, ps_value in field_checks:
                        if not ps_value is None:
                            if not mb_value is None:
                                if ps_value != mb_value:
                                    fields_to_be_updated[uniq_student_id][property] = ps_value
                            else:
                                fields_to_be_updated[uniq_student_id][property] = ps_value
                        else:
                            if not mb_value is None:
                                fields_to_be_updated[uniq_student_id][property] = None

                    for key in [k for k in mb_year_groups.keys() if not k == ps_stu.tables.students.grade]:
                        year_group = mb_year_groups[key]
                        if mb_student.get('id') in year_group.get('student_ids'):
                            execute(mb.endpoints.remove_from_year_group, records, f'{ps_stu.tables.students.grade} < {mb_student.get("student_id")}', id=year_group.get('id'), body={
                                'student_ids': [mb_student.get('id')]
                            })
                    # ensure removed from other year_groups
                else:
                    # no such ps_student, archive the student
                    execute(mb.endpoints.archive_a_student, records, mb_student.get('student_id'), id=mb_student.get('id'))

        print('SSs who need CLASSES to be REMOVED')
        for stu_id in to_be_removed:
            for class_id in to_be_removed[stu_id]:
                item = to_be_removed[stu_id][class_id]
                mb_student = item.student
                mb_class = item.clss
                execute(mb.endpoints.remove_students_from_class, records, f'{class_id} < {stu_id}', class_id=mb_class.get('id'), body={'student_ids': [mb_student.get('id')]})

        print('SS to be UPDATED')
        for stu_id in fields_to_be_updated:
            for property in fields_to_be_updated[stu_id]:
                mb_student = mb_students.get(stu_id) # session.query(Student).where(Student.student_id==stu_id).one()

                value = fields_to_be_updated[stu_id][property]
                if property == 'nationalities':
                    value = [value]

                body = {'student': {}}
                body['student'][property] = value
                execute(mb.endpoints.update_a_student, records, f'{stu_id}.{property} = {value}', id=mb_student.get('id'), body=body)

        print('SS to be ADDED to CLASS')
        # classes that student is supposed to be enrolled in according to PS, but not in MB yet
        academic_years = mb.endpoints.get_academic_years()

        for stud_id in ps_student_enrollments:
            ps_stu = ps_students.get(stud_id)
            ps_enrol = list(ps_student_enrollments[stud_id].keys())
            mb_enrol = mb_student_enrollments[stud_id]
            mb_student = mb_students.get(stud_id)

            if mb_student is None:
                continue  # dev, new students won't be there yet

            for add in set(ps_enrol) - set(mb_enrol):
                clss = mb_classes.get(add)
                if clss is None:
                    missing_classes.append({
                        'description': add,
                        'error': True,
                        'body': stu_id
                    })
                else:
                    # FIXME: Check that the class has begun, it's possible to be in the source but not intended to be enrolled in MB yet
                    # as it wouldn't be able to remove them, either
                    years = academic_years.get(clss.get('program_code'))
                    if not years:
                        continue
                    years = years.get('academic_years')
                    start_date = None
                    for terms in years:
                        for term in terms.get('academic_terms'):
                            if term.get('id') == clss.get('start_term_id'):
                                # use datetime as click's date param will need to be compared to it
                                start_date = datetime.datetime.fromisoformat(term.get('starts_on'))
                    assert start_date is not None, 'start_date cannot be None'
                    if start_date <= date:
                        execute(mb.endpoints.add_student_to_class, records, f'{stud_id} > {clss.get("uniq_id")}', class_id=clss.get('id'), body={'student_ids': [mb_student.get('id')]})
                    else:
                        records.append({
                            'description': f"{mb_student.get('student_id')} > {clss.get('uniq_id')}",
                            'error': False,
                            'change': False,
                            'body': 'Not enrolling as class has not begun',
                            'action': 'Enrol into class not yet started'
                        })

    finally:
        timestamp = f'{date_string}{postfix}'
        df = pd.DataFrame.from_records(records)
        df = df.sort_values(by='change', ascending=False)
        df.to_csv(f'/tmp/executions_{timestamp}.csv', index=False)

        df2 = pd.DataFrame.from_records(missing_classes)
        #df2 = df2.sort_values(by='change')
        df2.to_csv(f'/tmp/missing_classes_{timestamp}.csv', index=False)


        subject_description = ''
        change_description = ''
        error_description = ''
        num_errors = len(df.loc[df['error']])
        num_changes = len(df.loc[df['change']])
        if num_errors > 0:
            subject_description = f'{num_errors} errors'
            error_description = df.loc[df['error']]['action'].value_counts().sort_values(ascending=False).to_string()

        if num_changes > 0:
            subject_description += f' {num_changes} changes'
            change_description = df.loc[df['change']]['action'].value_counts().sort_values(ascending=False).to_string()

        body = ''
        if num_errors == 0 and num_changes == 0:
            body += 'Executed successfully. No changes needed, nor any errors encountered.'
        if num_errors > 0:
            body += f'Executed, but some errors happened:\n{error_description}\n\n'
        if num_changes > 0:
            body += f'Summary of changes made:\n{change_description}'

        if len(to_whom) > 0:
            send_email(
                user, to_whom, f'Sync Output {"(" if subject_description else ""}{subject_description.strip()}{")" if subject_description else ""}', body, password,
                ('sync_output.csv', df), ('missing_classes.csv', df2),
                *[(f'powerschool_{name}.csv', d) for name, d in [('students', psdf_students), ('parents', psdf_parents), ('enrollments', psdf_enrollments), ('teachers', psdf_teachers)]]
            )
        else:
            print(body)

    return records