#!/usr/bin/env python3
import os
import docker
import re
import tarfile
import yaml


class DockerUtils:
    @staticmethod
    def get_running_containers(project_name):
        client = docker.from_env()
        containers = client.containers.list(
            filters={"label": f"com.docker.compose.project={project_name}"}
        )
        if not containers:
            raise Exception(
                f"No running containers found for project '{project_name}'."
            )
        return containers

    @staticmethod
    def extract_files_from_container(handler, files_to_copy, destination):
        # Copy files from container to local directory
        container_name = handler.container_name
        for file in files_to_copy:
            try:
                # get_archive returns a tuple, 2nd element is just ignorable meta data
                data, _ = handler.container.get_archive(file)

                # Creating file name based on paths from above list of files to copy
                last_slash_index = file.rfind("/")
                file_name = file[last_slash_index + 1 :].lower()

                file_path = os.path.join(destination, file_name)
                with open(file_path, "wb") as f:
                    for chunk in data:
                        f.write(chunk)

                # get_archive extracts as a tar, so have to extract the inner stuff
                DockerUtils.extract_inner_file(file_path, destination)

                print(
                    f"Successfully copied {file_name} from {container_name} to {destination}"
                )

            except docker.errors.NotFound:
                print(f"File {file} not found in container {container_name}")
            except docker.errors.APIError as e:
                print(f"Error copying file: {e}")

    @staticmethod
    def build_image(dockerfile_path, image_name, tag):
        print(f"Building image from dockerfile at location: {dockerfile_path}")

        client = docker.from_env()
        try:
            image, build_logs = client.images.build(
                path=dockerfile_path,
                tag=f"qpadgham/{image_name}:{tag}",
                rm=True,
            )
            print(f"Successfully built image: {image.tags}")
        except docker.errors.BuildError as e:
            print(f"Build failed: {e}")
        except docker.errors.APIError as e:
            print(f"Error during API call: {e}")

    @staticmethod
    def extract_inner_file(filepath, extract_to="."):
        if not os.path.exists(filepath):
            print(f"File {filepath} does not exist.")
            return

        with tarfile.open(filepath, "r") as archive:
            inner_archive_filename = None
            for member in archive.getmembers():
                inner_archive_filename = member.name
                break

            if inner_archive_filename:
                inner_archive_file = archive.extractfile(inner_archive_filename)
                inner_archive_content = inner_archive_file.read()
                inner_archive_filepath = os.path.join(
                    extract_to, inner_archive_filename
                )
                with open(
                    inner_archive_filepath, "wb"
                ) as inner_archive_file_out:
                    inner_archive_file_out.write(inner_archive_content)
            else:
                print("Inner archive file not found within the outer archive.")

    @staticmethod
    def create_compose_file(image_name, handlers, destination_folder):
        # TODO, make this not Ignition specific
        docker_compose = {"services": {}}

        for handler in handlers:
            service_name = handler.container_name
            service_image_name = f"qpadgham/{image_name}:{service_name}"

            port_mapping = []
            ports = handler.container.ports
            for port in ports:
                if ports[port]:
                    new_mapping = ports[port][0]["HostPort"] + ":" + port
                    port_mapping.append(new_mapping)

            docker_compose["services"][service_name] = {
                "container_name": service_name,
                "image": service_image_name,
                "ports": port_mapping,
                "environment": handler.environment_variables,
                "deploy": handler.deploy,
            }

        compmose_filepath = os.path.join(
            destination_folder, "docker-compose.yml"
        )
        with open(compmose_filepath, "w") as file:
            yaml.dump(docker_compose, file)

        print(f"Generated docker-compose.yml at: {file}")


class ContainerHandler:
    """
    Every container image needs its own container_handler. The handler is
    responsible for preparing files in the container for extraction and
    providing a list of files to be extracted back to the get_files function.
    """

    def __init__(self, container):
        self.container = container
        self.container_name = (
            re.search(r"^[^-]+-([^-]+)-\d+$", container.name).group(1).lower()
        )
        self.image_name = container.image.tags[0].split(":")[0]
        self.image_tag = container.image.tags[0].split(":")[1]
        self.dockerfile_content = ""
        self.environment_variables = []
        self.deploy = None

    def prepare_files(self):
        raise NotImplementedError(
            "Method for extracting files not implemented for this container handler."
        )

    def create_dockerfile(self, folder):
        # Should call self.save_dockerfile at the end to save file once created
        raise NotImplementedError(
            "Method for creating dockerfile not implemented for this container handler."
        )

    def extract_resources(self, folder):
        files_to_copy = self.prepare_files()
        DockerUtils.extract_files_from_container(self, files_to_copy, folder)

    def create_derived_image(self, folder, image_name):
        """_summary_
        Args:
            folder (string): Folder where the docker file is located
            image_name (string): All derived images created for the entire
            build use the same image name, and use the tag to distinguish
            between them. Image name passed is the name for this build,
            container name is used as the tag for each individual image.
        """
        DockerUtils.build_image(folder, image_name, self.container_name)

    def save_dockerfile(self, folder):
        dockerfile_path = os.path.join(folder, "dockerfile")
        with open(dockerfile_path, "w") as dockerfile:
            dockerfile.write(self.dockerfile_content)
        print(f"Generated Dockerfile at: {dockerfile_path}")


class IgnitionHandler(ContainerHandler):
    def __init__(self, container):
        ContainerHandler.__init__(self, container)
        self.environment_variables = ["ACCEPT_IGNITION_EULA=Y"]

    def prepare_files(self):
        # Instruct gateway to create a gateway backup
        command = 'sh -c "/usr/local/bin/ignition/gwcmd.sh -y -b /usr/local/bin/ignition/data/backup.gwbk"'
        result = self.container.exec_run(command)
        if "backup saved" not in (result.output.decode("utf-8")):
            print(result.output.decode("utf-8"))
            print(f"Backup not created successfully for {self.container_name}")
            return
        else:
            print(f"Backup created successfully for {self.container_name}")

        # List of files to copy from container. Add more here if necessary
        files_to_copy = [
            "/usr/local/bin/ignition/data/redundancy.xml",
            "/usr/local/bin/ignition/data/.uuid",
            "/usr/local/bin/ignition/data/local/metro-keystore",
            "/usr/local/bin/ignition/data/backup.gwbk",
        ]
        return files_to_copy

    def create_dockerfile(self, folder):
        self.dockerfile_content = f"""\
FROM inductiveautomation/ignition:{self.image_tag}

COPY /.uuid /usr/local/bin/ignition/data/
COPY /metro-keystore /usr/local/bin/ignition/data/local/metro-keystore
COPY /redundancy.xml /usr/local/bin/ignition/data/redundancy.xml
COPY /backup.gwbk /restore.gwbk

USER root
RUN chown -R ignition /usr/local/bin/ignition/data/
RUN chgrp -R ignition /usr/local/bin/ignition/data/

RUN chown -R ignition /restore.gwbk
RUN chgrp -R ignition /restore.gwbk
USER ignition

ENTRYPOINT ["docker-entrypoint.sh", "-r", "/restore.gwbk"]
    """
        self.save_dockerfile(folder)


class ModbusHandler(ContainerHandler):
    def __init__(self, container):
        ContainerHandler.__init__(self, container)
        self.deploy = self.get_deploy()

    def nano_cpus_to_cpus(self, nano_cpus):
        return nano_cpus / 1e9

    def get_deploy(self):

        nano_cpus = self.container.attrs["HostConfig"].get("NanoCpus", 0)
        cpus = str(self.nano_cpus_to_cpus(nano_cpus))
        deploy = {"resources": {"limits": {"cpus": cpus}}}
        return deploy

    def prepare_files(self):
        # Instruct gateway to create a gateway backup
        files_to_copy = ["/setup.json"]
        return files_to_copy

    def create_dockerfile(self, folder):
        self.dockerfile_content = f"""\
FROM qpadgham/mymodbus:{self.image_tag}

COPY /setup.json /setup.json
    """
        self.save_dockerfile(folder)


class MSSQLHandler(ContainerHandler):
    def __init__(self, container):
        ContainerHandler.__init__(self, container)
        self.environment_variables = ["ACCEPT_EULA=Y"]
        self.get_sa_password()

    # Get SA password from container environment variables
    def get_sa_password(self):
        env_list = self.container.attrs["Config"]["Env"]
        for item in env_list:
            if item.startswith("SA_PASSWORD="):
                password = item.split("=", 1)[1]
                sa_password_env = f"SA_PASSWORD={password}"
                self.environment_variables.append(sa_password_env)

    def get_db_names(self):
        # Prompt the user for input
        input_string = input(
            f"Please enter a comma-separated list of database names for container {self.container_name}: "
        )

        # Split the input string by commas and strip any surrounding whitespace
        names = [name.strip() for name in input_string.split(",")]

        return names

    def get_latest_bak_files(self, files):
        file_dict = {}

        for file in files:
            # Extract the {some_string} and datetime part from the filename
            match = re.match(r"(.*)_(\d{8}_\d{6})\.bak", file)
            if match:
                some_string = match.group(1)
                date_str = match.group(2)

                # Update the dictionary with the latest date
                if (
                    some_string not in file_dict
                    or date_str > file_dict[some_string]
                ):
                    file_dict[some_string] = date_str

        # Construct the latest file names from the dictionary
        latest_files = [f"{key}_{file_dict[key]}.bak" for key in file_dict]
        return latest_files

    def prepare_files(self):
        # Instruct MSSQL to create a database backup
        db_names = self.get_db_names()
        for db_name in db_names:
            command = f'sh -c "./backup.sh {db_name}"'
            result = self.container.exec_run(command)
            if "BACKUP DATABASE" not in (result.output.decode("utf-8")):
                print(result.output.decode("utf-8"))
                print(
                    f"Backup not created successfully for {self.container_name}'s database {db_name}"
                )
                return
            else:
                print(
                    f"Backup created successfully for {self.container_name}'s database {db_name}"
                )

        # Returns full filepaths for all files in the /backups folder, which
        # should include a backup for each database entered in get_db_names()
        find_backups_cmd = f'sh -c "find /backups -type f"'
        result = self.container.exec_run(find_backups_cmd)

        # Return of the find command is a long string, split by line
        all_baks = result.output.decode("utf-8").splitlines()

        # There could multiple backups for a single database if they happened
        # to create some already. Want to only pass the most recent for each
        # uniquely named file.
        files_to_copy = self.get_latest_bak_files(all_baks)

        return files_to_copy

    def create_dockerfile(self, folder):

        bak_files = os.listdir(folder)

        # bak_files have a timestamp in the name, but the filename has to
        # only include the db name to properly match to the db name that
        # existed in the previous file
        shortened_filenames = []
        for file in bak_files:
            base_name = os.path.basename(file)
            match = re.match(r"^(.*?)_\d{8}_\d{6}\.bak$", base_name)
            if match:
                new_name = match.group(1) + ".bak"
                shortened_filenames.append(new_name)

        # This ugly string format creates a new copy and chown line for each
        # bak file extracted by the handler in the prepare_files() function
        self.dockerfile_content = """
FROM kcollins/mssql:{}

{}
USER root
{}
USER mssql
""".format(
            self.image_tag,
            "\n".join(
                [
                    f"COPY /{file_path} /docker-entrypoint-initdb.d/{shortened_filename}"
                    for file_path, shortened_filename in zip(
                        bak_files, shortened_filenames
                    )
                ]
            ),
            "\n".join(
                [
                    f"RUN chown mssql /docker-entrypoint-initdb.d/{shortened_filename}"
                    for shortened_filename in shortened_filenames
                ]
            ),
        )
        self.save_dockerfile(folder)


class BuildManager:
    def __init__(self, project_name, image_name, destination_folder=os.getcwd()):
        self.project_name = project_name
        self.hanlders = self.get_handlers()
        self.base_folder = destination_folder
        self.image_name = image_name

    def get_handlers(self):
        containers = DockerUtils.get_running_containers(self.project_name)
        handlers = []
        for container in containers:
            try:
                handler = HandlerFactory.get_handler(container)
                handlers.append(handler)
            except Exception as e:
                print(
                    f"No handler found for {container.name}, skipping. Error: {e}"
                )
        return handlers

    def create_container_folder(self, handler):
        folder_string = handler.container_name
        folder_string_lower = folder_string.lower()
        container_folder = os.path.join(self.base_folder, folder_string_lower)
        os.makedirs(container_folder, exist_ok=True)
        return container_folder

    def create_derived_images(self):
        for handler in self.hanlders:
            folder = self.create_container_folder(handler)
            handler.extract_resources(folder)
            handler.create_dockerfile(folder)
            handler.create_derived_image(folder, self.image_name)

    def create_compose_file(self):
        DockerUtils.create_compose_file(
            self.image_name, self.hanlders, self.base_folder
        )

    def create_build_image(self):
        build_dockerfile_content = f"""
FROM docker

#move compose file in and make it accessible
COPY /docker-compose.yml /usr/local/bin/{self.image_name}/docker-compose.yml
RUN chmod +x  /usr/local/bin/{self.image_name}/docker-compose.yml

#move edge shark comopse file and make it accessible
COPY /docker-compose_WS.yml /usr/local/bin/edgeshark/docker-compose.yml
RUN chmod +x /usr/local/bin/edgeshark/docker-compose.yml

#move entry shim
COPY /entrypoint-shim.sh /usr/local/bin/entrypoint-shim.sh
RUN chmod +x /usr/local/bin/entrypoint-shim.sh

ENTRYPOINT [ "/usr/local/bin/entrypoint-shim.sh" ]"""

        dockerfile_path = os.path.join(self.base_folder, "dockerfile")
        with open(dockerfile_path, "w") as file:
            file.write(build_dockerfile_content)
        DockerUtils.build_image(self.base_folder, self.image_name, "build")

    def create_shim(self):
        shim_content = f"""\
#!/bin/sh
set -euo pipefail
echo "Starting compose script and normal entry point..."

#start the normal entrypoint in the background
/usr/local/bin/docker-entrypoint.sh "$@" &

while ! docker ps
do
sleep 2
done

#now that docker is running, compose up
docker compose -f  /usr/local/bin/{self.image_name}/docker-compose.yml up -d

#compose up edge shark
docker compose -f /usr/local/bin/edgeshark/docker-compose.yml up -d

#kick off built in entrypoint
exit 0
        """
        shim_path = os.path.join(self.base_folder, "entrypoint-shim.sh")
        with open(shim_path, "w", newline="\n") as shim:
            shim.write(shim_content)

        print(f"Generated shim at: {shim_path}")

    def run(self):
        self.create_derived_images()
        self.create_compose_file()
        self.create_shim()
        self.create_build_image()


class HandlerFactory:
    handlers = {
        "inductiveautomation/ignition": IgnitionHandler,
        "kcollins/mssql": MSSQLHandler,
        "qpadgham/mymodbus": ModbusHandler,
    }

    class HandlerNotFoundError(Exception):
        def __init__(self, image_name):
            self.image_name = image_name
            self.message = f"No handler found for image: {image_name}"
            super().__init__(self.message)

    @staticmethod
    def get_handler(container):
        image_name = container.image.tags[0].split(":")[0]
        if image_name in HandlerFactory.handlers:
            return HandlerFactory.handlers[image_name](container)
        else:
            raise HandlerFactory.HandlerNotFoundError(image_name)


if __name__ == "__main__":
    manager = BuildManager("modbus", "modbus")
    manager.run()
