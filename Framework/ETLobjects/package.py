import os
from typing import List, Dict, Union
from Framework.special_expansion.special_functions import catch_problem
from Framework.special_expansion import allias_types
from Framework.ETLobjects.package_object import PackageObject, PackageObjectsList


class Package:
    """
    Класс для работы с пакетом задачи (содержит в себе информацию об объектах). В том числе парсит пакет
    """

    def __init__(self, package_name: str, package_directory_path: str, work_directory_path: str,
                 scenario: str = 'Release'):
        self._ThisPackage = PackageObject(package_name, work_directory_path, package_directory_path)
        self._Scenario = scenario
        self._ObjectsList: PackageObjectsList = PackageObjectsList()
        self._MetaObjectsListBeforeAlter = None
        self._TaskList: allias_types.TaskList = []

    @property
    def PackageName(self) -> str:
        return self._ThisPackage.Name

    @property
    def PackageNumber(self) -> str:
        return self._ThisPackage.Name.split('-')[1]

    @property
    def ThisPackage(self) -> PackageObject:
        return self._ThisPackage

    @property
    def BackupSpkFilesPath(self) -> str:
        path = os.path.join(self._ThisPackage.Path, 'backup')
        return path

    @property
    def BufferSpkFilesPath(self) -> str:
        buffer_path = os.path.join(self._ThisPackage.Path, 'buffer')
        return buffer_path

    @property
    def ReleaseSpkFilesPath(self) -> str:
        path = os.path.join(self._ThisPackage.Path, 'release', 'spk')
        return path

    def addMetaToPackage(self, name: str, location: str, path: str, flags: str, type: str, metaobject: bool,
                         phys_table: str):
        new_object = PackageObject(name, location, path, flags, type, metaobject, phys_table)
        self._ObjectsList.addObject(new_object)

    def getObject(self, name) -> List[PackageObject]:
        return self._ObjectsList.getObject(name)

    def addFileToPackage(self, file_name: str, file_path: str, file_type: str):
        """
        Добавляю файл в список объектов пакета
        :param file_name: имя файла
        :param file_path: путь к файлу
        :param file_type: тип файла
        """
        new_object = PackageObject(file_name, file_path, os.path.join(file_path, file_name), type=file_type.lower())
        self._ObjectsList.addObject(new_object)

    def addTask(self, task_name: str, task_position: str, task_objects: List[str], additional_options: Dict[str, str]):
        """
        Добавлю таск в список тасков
        :param additional_options:
        :param task_name: имя таска
        :param task_position: позиция таска в кастомном сценарии
        :param task_objects: объекты, над которыми работает таск
        """
        task = {'name': task_name, 'position': task_position, 'objects': task_objects,
                'additional_options': additional_options}
        self._TaskList.append(task)

    def getTasksByName(self, task_name: str) -> allias_types.TaskList:
        """
        :param task_name: имя таска
        """
        tasks: allias_types.TaskList = []

        for task in self._TaskList:
            if task.get('name') == task_name:
                tasks.append(task)

        return tasks

    def getTaskObjects(self, task_name: str, task_position: str) -> List[str]:
        """
        Верну таск
        :param task_name: имя таска
        :param task_position: позиция таска в кастомном сценарии
        """
        task_objects: List[str] = []
        for task in self._TaskList:
            if task.get('name') == task_name and task.get('position') == task_position:
                task_objects = task.get('objects')

        return task_objects

    def getObjectsByTaskName(self, task_name: str) -> List[str]:
        """
        Возвращает список объектов objects из всех тасков task_name
        :param task_name: имя таска
        """
        objects: List[str] = []
        for task in self._TaskList:
            if task.get('name') == task_name:
                objects.extend(task.get('objects'))

        return objects

    def getTaskAdditionalOptions(self, task_name: str) -> List[Dict[str, str]]:
        """
        Возвращает список объектов additional_options из всех тасков с именем task_name
        :param task_name: имя таска в кастомном сценарии
        """
        merged_objects: List[Dict[str, str]] = []
        for task in self._TaskList:
            if task.get('name') == task_name:
                merged_objects.append(task.get('additional_options'))

        return merged_objects

    def splitMetaPath(self, meta_path: str) -> Union[str, List[str]]:
        meta_path_parts = meta_path.rsplit('/', 1)
        return meta_path_parts if len(meta_path_parts) == 2 else ('', meta_path)

    @catch_problem
    def _findMetaObjectsBeforeAlter(self, meta_after_alter: List[PackageObject]):
        self._MetaObjectsListBeforeAlter = PackageObjectsList()
        altered_meta_map: Dict[str, str] = {}

        for task in self.getTasksByName('AlterMetadata'):
            task_objects = task.get('objects')
            if len(task_objects) > 0:
                altered_meta_map[task_objects[0]] = task.get('additional_options')['old_meta_name_and_path']

        for meta_object in meta_after_alter:
            if meta_object.Name in altered_meta_map:
                meta_path = altered_meta_map[meta_object.Name]
            else:
                meta_path = meta_object.Path

            meta_location, meta_name = self.splitMetaPath(meta_path)

            object_before_alter = PackageObject(meta_name, meta_location, meta_path,  flags=meta_object.Flags,
                                                type=meta_object.Type, metaobject=True, new_meta_object=meta_object)
            self._MetaObjectsListBeforeAlter.addObject(object_before_alter)
            meta_object.OldMetaObject = object_before_alter

    def parseConfig(self, config_file: PackageObject):
        with open(config_file.Path, 'r') as f_in:
            for line in f_in:
                if len(line.strip()) > 0:
                    meta_path = line[:line.rindex('('):]

                    meta_location, meta_name = self.splitMetaPath(meta_path)
                    meta_type = line.split('(')[-1].split(')')[0]
                    meta_flags = line.split('{')[-1].split('}')[0]

                    print('Found meta object: {0}({1})'.format(meta_name, meta_type))
                    new_meta_object = PackageObject(meta_name, meta_location, meta_path, flags=meta_flags,
                                                    type=meta_type,
                                                    metaobject=True)

                    self._ObjectsList.addObject(new_meta_object)

    def parsePackage(self):
        """
        Парсим пакет задачи
        """
        self._ObjectsList = PackageObjectsList()
        print('Looking for files in a directory "{0}"'.format(self._ThisPackage.Path))

        for dir, dirs, files in os.walk(self._ThisPackage.Path):
            # Не заходим в каталог .svn
            if dir.find('.svn') == -1:
                for file_name in files:
                    extension = file_name.split('.')[-1]
                    new_object = PackageObject(file_name, dir, os.path.join(dir, file_name), type=extension.lower())
                    self._ObjectsList.addObject(new_object)

        # Находим файл config.hat чтобы прочитать метаданные
        if self._Scenario.startswith('DevialAction') or self._Scenario == 'BiSynchronize':
            config_files = self.getFiles(filename='.tmp_config.hat', type='hat')
        else:
            config_files = self.getFiles(filename='config.hat', type='hat')

        if self._Scenario == 'Devial':
            config_files.extend(self.getFiles(filename='.dev_config.hat', type='hat'))

        if len(config_files) > 0:
            for config_file in config_files:
                self.parseConfig(config_file)
        else:
            raise Exception('Not found file "config.hat"!')

        meta_found: int = 0
        file_found: int = 0

        for package_object in self._ObjectsList.getObjects():

            if package_object.MetaObject:
                meta_found += 1
            else:
                file_found += 1

        print('LOG: Found files: {0}'.format(file_found))
        print('LOG: Read metadata: {0}'.format(meta_found))

    def getCountObjects(self) -> int:
        """
        Верну количество объектов в пакете
        """
        return self._ObjectsList.getCountObjects()

    def getAllObjects(self) -> List[PackageObject]:
        """
        Верну все объекты в пакете
        """
        return self._ObjectsList.getObjects()

    @catch_problem
    def _getMetaObjects(self, objects_list: PackageObjectsList, filters: str) -> List[PackageObject]:
        """
        Верну метаданные из objects_list по фильтру.
        :param filters: Фильтр, примеры:
            "Job{i}" - джобы с флагом i
            "Meta{i,n}" - все объекты с флагами i и n
            "Job{i}|Table{b}" - джобы с флагом i и таблицы с флагом b
            "Meta{b,r}" - любая мета с флагом b и r
            "Table{b}|Table{n}" - любая таблица с флагом b или n
            "Job{i,!n}" - джобы с флагом i, но без флага n
            "Job{!i,!n}" - все джобы без флагов i и n
            "Meta{}" - все метаданные
        :return: список метаданных
        """
        all_filters: allias_types.Filters = []
        for filter in filters.split('|'):
            filter_dict = {
                'type': filter.split('{')[0],
                'flags': filter.split('{')[-1].split('}')[0].split(',')
            }
            all_filters.append(filter_dict)

        result: List[PackageObject] = []
        for meta_object in objects_list.getMetaObjects():
            for filter in all_filters:
                if meta_object.Type == filter.get('type') or filter.get('type') == 'Meta':
                    adding = True

                    for flag in filter.get('flags'):
                        if (flag.find('!') == -1 and meta_object.Flags.find(flag.replace('!', '')) == -1) or \
                                (flag.find('!') > -1 and meta_object.Flags.find(flag.replace('!', '')) > -1):
                            adding = False
                            break

                    if adding:
                        if meta_object not in result:
                            result.append(meta_object)

        return result

    def getMetaObjectsBeforeAlter(self, filters: str) -> List[PackageObject]:
        """
        Верну метаданные до переименований по фильтру.
        """
        if self._MetaObjectsListBeforeAlter is None:
            self._findMetaObjectsBeforeAlter(self.getMetaObjects('Meta{}'))

        return self._getMetaObjects(self._MetaObjectsListBeforeAlter, filters)

    def getMetaObjects(self, filters: str) -> List[PackageObject]:
        """
        Верну метаданные после переименований по фильтру.
        """
        return self._getMetaObjects(self._ObjectsList, filters)

    def getFiles(self, filename: str = None, type: str = None) -> List[PackageObject]:
        """
        Верну список файлов по маске имени и/или по типу файла. Если оба параметра None - верну все файлы
            :param filename: имя файла целиком (по умолчанию None)
            :param type: тип файла (по умолчанию None)
            :return: список файлов
        """
        result: List[PackageObject] = []
        for package_object in self._ObjectsList.getFileObjects():
            if filename is None or package_object.Name.lower() == filename.lower():
                if type is None or type.lower() == package_object.Type.lower():
                    result.append(package_object)

        return result

    @catch_problem
    def getObjects(self, objects_list: List[str]) -> List[PackageObject]:
        """
        Верну объекты пакета (список из экземпляров класса PackageObject)
            :param objects_list: список объектов (их имена), например: ['DDS LOAD MAILS', 'config.hat']
        """
        result: List[PackageObject] = []
        all_objects_in_package = self._ObjectsList.getObjects()

        for in_object in objects_list:
            for package_object in all_objects_in_package:
                if in_object.lower().strip() == package_object.Name.lower().strip():
                    result.append(package_object)

        return result


class PackageChimeraAdapter(Package):

    def parsePackage(self, chimera_api):
        """
        Парсим пакет задачи
        """
        self._ObjectsList = PackageObjectsList()
        print('Looking for objects in Chimera')

        files = chimera_api.get_package_files(self.PackageName).get('items')

        if files:
            for file in files:
                if file['is_directory']: continue

                name = file['name']
                location = file['path']
                path = (location + '/' + name) if location else name
                name_parts = name.rpartition('.')
                file_ext = name_parts[2].lower() if name_parts[1] else None
                new_object = PackageObject(name, location, path, type=file_ext)
                self._ObjectsList.addObject(new_object)

        # Получаем объекты задачи
        self.parseConfig(chimera_api)

        meta_found: int = 0
        file_found: int = 0

        for package_object in self._ObjectsList.getObjects():

            if package_object.MetaObject:
                meta_found += 1
            else:
                file_found += 1

        print('LOG: Found files: {0}'.format(file_found))
        print('LOG: Read metadata: {0}'.format(meta_found))

    def parseConfig(self, chimera_api):
        self._MetaObjectsListBeforeAlter = PackageObjectsList()
        task_objects = chimera_api.get_package_objects(self.PackageName).get('items')

        types_matching = {'tedi': 'Dag',
                          'gp': 'GpTable',
                          'dlh': 'DlhTable',
                          'cut2': 'Cut2',
                          'uni': '',
                          'nifi': '',
                          'dummy': ''}

        for task_object in task_objects:
            n_flag = ''
            if task_object['is_new']:
                n_flag = 'n'

            flags = n_flag + ''
            for flag in task_object['flags']:
                flags = flags + flag[0]

            meta_name_new = task_object['name']
            meta_name_old = meta_name_new
            if task_object.get('new_name'):
                meta_name_new = task_object['new_name']

            meta_type = types_matching.get(task_object['plugin'])
            print('Found meta object: {0}({1})'.format(meta_name_new, meta_type))

            new_meta_object = PackageObject(meta_name_new, '', '', flags=flags, type=meta_type, metaobject=True)
            old_meta_object = PackageObject(meta_name_old, '', '', flags=flags, type=meta_type, metaobject=True)

            new_meta_object.OldMetaObject = old_meta_object
            old_meta_object.NewMetaObject = new_meta_object

            self._ObjectsList.addObject(new_meta_object)
            self._MetaObjectsListBeforeAlter.addObject(old_meta_object)

    def addTask(self, step_id: str, step_type: str, step_index: str, parameters: List[dict], stage: str, plugin: str, additional_options: Dict[str, str]):
        """
        Добавлю таск в список тасков
        :param step_id:
        :param step_type: имя таска
        :param step_index: позиция таска
        :param parameters: объекты, над которыми работает таск
        :param stage: стадия релиза
        :param plugin: плагин, с которым взаимодействует таск
        :param additional_options:
        """
        task = {'id': step_id, 'type': step_type, 'index': step_index,
                'parameters': parameters, 'stage': stage, 'plugin': plugin,
                'additional_options': additional_options}

        objects_svn = []
        for parameter in task['parameters']:
            if parameter.get('allowed_values'):
                for allowed_value in parameter['allowed_values']:
                    if allowed_value.get('value'):
                        objects_svn.append(allowed_value['value'])
            if parameter['name'] == 'table_name':
                objects_svn.append(parameter['value'])

        additional_options_svn = {'stage': task['stage'], 'plugin': task['plugin']}
        if task.get('additional_options'):
            additional_options_svn.update(task['additional_options'])

        task_svn = {'name': task['type'], 'position': task['index'], 'objects': objects_svn,
                    'additional_options': additional_options_svn}

        self._TaskList.append(task_svn)
