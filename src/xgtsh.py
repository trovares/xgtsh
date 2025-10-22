#! /usr/bin/env python
# -*- coding: utf-8 -*- --------------------------------------------------===#
#
#  Copyright 2023 Trovares Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#===----------------------------------------------------------------------===#

import argparse
import cmd
import getpass
import os
import pprint
import sys
import warnings

import xgt

try:
  import pandas as pd
  HASPANDAS=True
except:
  HASPANDAS = False

# import the linux or windows version of readline for auto completion
READLINE_DEFINED = False
try:
    import readline
    READLINE_DEFINED = True
except:
    pass
try:
    import pyreadline
    READLINE_DEFINED = True
except:
    pass

#----------------------------------------------------------------------------

class XgtCli(cmd.Cmd):
  """
  Command-line console for Trovares xGT.
  """
  original_prompt = 'xGT>> '
  prompt = original_prompt

  def __init__(self, host, port, username, password = None, verbose = False, debug = False):
    super().__init__()

    self.__username = username
    self.__password = password
    self.__port = port
    self.__hostname = host
    self.__verbose = verbose
    self.__debug = debug
    if self.__debug:
      warnings.simplefilter("default")
    else:
      warnings.simplefilter("ignore")
    self.__server = self.__connect_to_server()

    if READLINE_DEFINED:
      # --- make sure tab completion works on a MAC
      if 'libedit' in readline.__doc__:
        readline.parse_and_bind("bind ^I rl_complete")
      self.__old_completer_delims = readline.get_completer_delims()
      readline.set_completer_delims(' =')
      # ----- keep history of commands between runs
      histfile = os.path.join(os.path.expanduser("~"), ".xgthist")
      try:
        readline.read_history_file(histfile)
      except IOError:
        pass
      import atexit
      atexit.register(readline.write_history_file, histfile)

  def __del__(self):
    if READLINE_DEFINED and hasattr(self, '_XgtCli__old_completer_delims'):
      readline.set_completer_delims(self.__old_completer_delims)

  def emptyline(self):
    pass

  def _namespace_complete(self, text, line, begidx, endidx) -> tuple:
    if self.__verbose:
      print(f"\nnamespace_completion: text: {text}, line: {line}, begidx: {begidx}, endidx: {endidx}")

    if self.__server is None:
      return []

    namespaces = self.__server.get_namespaces()
    return [_ for _ in namespaces if _.startswith(text)]


  def do_cancel(self, line)->bool:
    """Cancel a job"""
    if self.__server is None:
      print("Not connected to a server")
      return False
    fields = line.split()
    if len(fields) < 1:
      print(f"Usage: {self.prompt} cancel <job-id>")
      return False
    try:
      job_id = int(fields[0])
    except:
      print(f"Usage: {self.prompt} cancel <job-id>")
      print("where:  <job-id> must be an integer")
      return False
    self.__server.cancel_job(job_id)
    return False

  def do_config(self, line)->bool:
    """Show server configuration"""
    if self.__server is None:
      print("Not connected to a server")
    else:
      fields = line.split()
      if len(fields) < 1:
        config = self.__server.get_config()
      elif fields[0] == 'set' and fields[2] == '=':
        param = fields[1]
        value = fields[3]
        if value.lower() == 'false' or value.lower() == 'true':
          value = value.lower() == 'true'
        elif value.isnumeric() or (value[0] == '-' and value[1:].isnumeric()):
          value = int(value)
        try:
          self.__server.set_config({param: value})
        except xgt.XgtError as e:
          print(f"Error: {e}")
        return False
      else:
        print(f"Unknown config parameters: {fields}")
        return False

      print("\n".join([f"{k} = {v}" for k,v in sorted(config.items())]))
    return False

  def do_debug(self, line)->bool:
    """Set debug on or off"""
    if len(line) > 1 and line.lower() == "on":
      self.__debug = True
      warnings.simplefilter("default")
    else:
      self.__debug = False
      warnings.simplefilter("ignore")
    return False

  def do_default_namespace(self, line)->bool:
    """Show or set default namespace"""
    if len(line) < 1:
      print(f"Default namespace: {self.__server.get_default_namespace()}")
    else:
      self.__server.set_default_namespace(line)
    return False
  complete_default_namespace = _namespace_complete

  def do_drop(self, line)->bool:
    """Drop a frame"""
    if len(line) < 1:
      print(f"Command:  DROP <frame-name>")
    else:
      self.__server.drop_frame(line)
    return False

  def do_exit(self, line)->bool:
    """
    Exit the console.

    :param line: the line of text passed with the command

    :return: True to exit
    """
    return True

  def do_EOF(self, line)->bool:
    """Exit the console"""
    print("")
    return self.do_exit(line)

  def do_job(self, line)->bool:
    """
    Show detail information on a job

      xGT>> job <job-number>
      xGT>> job <start-job-number> (<end-job-number>)

    """
    if self.__server is None:
      print("Not connected to a server")
      return False
    self.__process_job_command(line)
    return False

  def do_jobs(self, line)->bool:
    """
    Show summary information on jobs

      xGT>> jobs (<state>)

    If the optional <state> parameter is provided, show only jobs in that state
    """
    if self.__server is None:
      print("Not connected to a server")
      return False
    jobs = self.__server.get_jobs()
    jobs_map = {_.id:_ for _ in jobs}
    for job_id in sorted([_.id for _ in jobs]):
      job = jobs_map[job_id]
      if len(line) < 1 or job.status == line:
        print(f"{job_id:3d}: {jobs_map[job_id]}")
    return False

  def do_memory(self, line)->bool:
    """Show server memory status"""
    if self.__server is None:
      print("Not connected to a server")
    else:
      max_memory = self.__server.max_user_memory_size
      footprint = max_memory - self.__server.free_user_memory_size
      print(f"Current RAM footprint: {footprint:,.3f} GiB used out of {max_memory:,.3f} GiB available.")
    return False

  def do_namespaces(self, line)->bool:
    """Show current namespaces"""
    if self.__server is None:
      print("Not connected to a server")
    else:
      namespaces = self.__server.get_namespaces()
      print(", ".join(namespaces))
    return False

  def do_query(self, line:str)->bool:
    """Run a query"""
    if self.__server is None:
      print("Not connected to a server")
    else:
      job = self.__server.run_job(line)
      if HASPANDAS:
        data = job.get_data()
        # Extract column names from job schema if available
        columns = None
        if hasattr(job, 'schema') and job.schema:
          columns = [field[0] for field in job.schema]
        df = pd.DataFrame(data, columns=columns)
        print(df)
      else:
        data = job.get_data()
        pprint.pprint(data)
    return False

  def do_save(self, line:str)->bool:
    """Save a frame to a file"""
    fields = line.split()
    if len(fields) < 2:
      print(f"Usage: {self.prompt} save <frame-name> <filename>")
      return False
    frame_name = fields[0]
    filename = fields[1]
    try:
      frame = self.__server.get_frame(frame_name)
    except:
      print(f"Frame {frame_name} does not exist")
      return False
    frame.save(filename, headers=True)
    return False

  def do_schema(self, line)->bool:
    """Show schema of specified frame"""
    frame_name = line
    print(f"Showing schema of frame {frame_name}")
    try:
      frame = self.__server.get_frame(frame_name)
    except:
      print(f"Frame {frame_name} does not exist")
      return False

    schema = frame.schema
    print(f"Schema: {schema}")

    # Check if it's an edge frame by examining the frame type
    if hasattr(frame, 'source_name') and hasattr(frame, 'target_name'):
      print(f"Source frame: {frame.source_name}, Target frame: {frame.target_name}")
    return False

  def do_scroll(self, line)->bool:
    """Scroll through frame data"""
    frame_name = line
    try:
      frame = self.__server.get_frame(frame_name)
    except:
      print(f"Frame {frame_name} does not exist")
      return False

    offset = 0
    data = frame.get_data(offset, 20)
    print("Data:")
    for row in data:
      pprint.pprint(row)
    return False

  def do_show(self, line)->bool:
    """Show graph information in the running xGT server"""
    if self.__server is None:
      print("Not connected to a server")
      return False
    fields = line.split()
    if len(fields) < 1:
      print(f"Usage: {self.prompt} show <namespace>")
      return False
    ns = str(fields[0])

    tables = self.__server.get_frames(namespace=ns, frame_type='table')
    total_table_rows = 0
    for table in tables:
      if self.__verbose or not table.name.startswith('xgt__'):
        total_table_rows += table.num_rows
        acl_str = self.__get_frame_labels_str(table.name)
        print(f"TableFrame {table.name} has {table.num_rows:,} rows{acl_str}")
    print(f"Total table rows over all frames: {total_table_rows:,}")
    vertices = self.__server.get_frames(namespace=ns, frame_type='vertex')
    total_vertices = 0
    for vertex in vertices:
      if self.__verbose or not vertex.name.startswith('xgt__'):
        total_vertices += vertex.num_rows
        acl_str = self.__get_frame_labels_str(vertex.name)
        print(f"VertexFrame {vertex.name} has {vertex.num_vertices:,} vertices{acl_str}")
    print(f"Total vertices over all frames: {total_vertices:,}")
    edges = self.__server.get_frames(namespace=ns, frame_type='edge')
    total_edges = 0
    for edge in edges:
      if self.__verbose or not edge.name.startswith('xgt__'):
        total_edges += edge.num_edges
        acl_str = self.__get_frame_labels_str(edge.name)
        print(f"EdgeFrame {edge.name} has {edge.num_edges:,} edges{acl_str}")
    print(f"Total edges over all frames: {total_edges:,}")

    return False
  complete_show = _namespace_complete

  def do_show_frames(self, line)->bool:
    """Show all frames in the default namespace"""
    if self.__server is None:
      print("Not connected to a server")
      return False

    default_ns = self.__server.get_default_namespace()

    # Get all frame types using the latest API
    tables = self.__server.get_frames(namespace=default_ns, frame_type='table')
    vertices = self.__server.get_frames(namespace=default_ns, frame_type='vertex')
    edges = self.__server.get_frames(namespace=default_ns, frame_type='edge')

    print(f"Frames in namespace '{default_ns}':")
    print()

    if tables:
      print("Table Frames:")
      for table in tables:
        if self.__verbose or not table.name.startswith('xgt__'):
          print(f"  {table.name} ({table.num_rows:,} rows)")

    if vertices:
      print("Vertex Frames:")
      for vertex in vertices:
        if self.__verbose or not vertex.name.startswith('xgt__'):
          print(f"  {vertex.name} ({vertex.num_vertices:,} vertices)")

    if edges:
      print("Edge Frames:")
      for edge in edges:
        if self.__verbose or not edge.name.startswith('xgt__'):
          print(f"  {edge.name} ({edge.num_edges:,} edges)")

    if not tables and not vertices and not edges:
      print("  No frames found")

    return False

  def do_verbose(self, line)->bool:
    """Turn verbose setting on/off"""
    fields = line.split()
    if len(fields) < 1 or fields[0].lower() != 'off':
      self.__verbose = True
    else:
      self.__verbose = False
    return False

  def do_version(self, line)->bool:
    """Show version information"""
    print(f"Client version: {xgt.__version__}")
    if self.__server is None:
      print("Server is not connected")
    else:
      print(f"Server version: {self.__server.server_version}")
    return False

  def do_user_labels(self, line)->bool:
    """Show the current user's security labels"""
    if self.__server is None:
      print("Not connected to a server")
      return False
    try:
      labels = self.__server.get_user_labels()
      if labels:
        print("User security labels:")
        for label in labels:
          print(f"  {label}")
      else:
        print("User has no security labels")
    except xgt.XgtError as e:
      print(f"Error retrieving user labels: {e}")
    return False

  def do_zap(self, line)->bool:
    """Zap a namespace"""
    if self.__server is None:
      print("Not connected to a server")
      return False
    fields = line.split()
    if len(fields) < 1:
      print(f"Usage: {self.prompt} zap <namespace>")
      return False
    ns = str(fields[0])

    if self.__version_is_since(1, 14, 0):
      frames = self.__server.get_frames(namespace=ns)
      self.__server.drop_frames(frames)
      deleted_frames = len(frames)
    else:
      edges = self.__server.get_edge_frames(namespace=ns)
      for edge in edges:
        self.__server.drop_frame(edge)
        if self.__verbose:
          print(f"EdgeFrame {edge.name} deleted")
      self.__server.wait_for_metrics()
      tables = self.__server.get_table_frames(namespace=ns)
      for table in tables:
        self.__server.drop_frame(table)
        if self.__verbose:
          print(f"TableFrame {table.name} deleted")
      vertices = self.__server.get_vertex_frames(namespace=ns)
      for vertex in vertices:
        self.__server.drop_frame(vertex)
        if self.__verbose:
          print(f"VertexFrame {vertex.name} deleted")
      deleted_frames = len(tables) + len(vertices) + len(edges)

    print(f"Deleted {deleted_frames} frames in namespace {ns}")
    return False
  complete_zap = _namespace_complete

  #------------------------- Non-interactive execution methods

  def execute_query_and_exit(self, query: str, format: str = 'table', namespace: str = None) -> None:
    """Execute a single Cypher query and exit"""
    if self.__server is None:
      print("Not connected to a server", file=sys.stderr)
      sys.exit(1)

    try:
      # Set namespace if specified
      if namespace:
        self.__server.set_default_namespace(namespace)
        if self.__verbose:
          print(f"Set default namespace to: {namespace}")

      job = self.__server.run_job(query)
      data = job.get_data()

      if format == 'json':
        import json
        print(json.dumps(data, indent=2))
      elif format == 'csv':
        if HASPANDAS:
          # Extract column names from job schema if available
          columns = None
          if hasattr(job, 'schema') and job.schema:
            columns = [field[0] for field in job.schema]
          df = pd.DataFrame(data, columns=columns)
          print(df.to_csv(index=False))
        else:
          import csv
          import io
          output = io.StringIO()
          if data:
            writer = csv.DictWriter(output, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
            print(output.getvalue())
      else:  # table format (default)
        if HASPANDAS:
          # Extract column names from job schema if available
          columns = None
          if hasattr(job, 'schema') and job.schema:
            columns = [field[0] for field in job.schema]
          df = pd.DataFrame(data, columns=columns)
          print(df)
        else:
          pprint.pprint(data)
    except Exception as e:
      print(f"Error executing query: {e}", file=sys.stderr)
      sys.exit(1)

  def execute_command_and_exit(self, command: str, namespace: str = None) -> None:
    """Execute a single shell command and exit"""
    if self.__server is None:
      print("Not connected to a server", file=sys.stderr)
      sys.exit(1)

    try:
      # Set namespace if specified
      if namespace:
        self.__server.set_default_namespace(namespace)
        if self.__verbose:
          print(f"Set default namespace to: {namespace}")

      # Parse the command and execute it
      parts = command.split(None, 1)
      if not parts:
        return

      cmd_name = parts[0]
      cmd_args = parts[1] if len(parts) > 1 else ""

      # Get the method for this command
      method_name = f"do_{cmd_name}"
      if hasattr(self, method_name):
        method = getattr(self, method_name)
        method(cmd_args)
      else:
        print(f"Unknown command: {cmd_name}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
      print(f"Error executing command: {e}", file=sys.stderr)
      sys.exit(1)

  def execute_file_and_exit(self, filename: str) -> None:
    """Execute commands from a file and exit"""
    if self.__server is None:
      print("Not connected to a server", file=sys.stderr)
      sys.exit(1)

    try:
      with open(filename, 'r') as f:
        for line_num, line in enumerate(f, 1):
          line = line.strip()
          if not line or line.startswith('#'):
            continue

          try:
            # Parse and execute each command
            parts = line.split(None, 1)
            if not parts:
              continue

            cmd_name = parts[0]
            cmd_args = parts[1] if len(parts) > 1 else ""

            # Get the method for this command
            method_name = f"do_{cmd_name}"
            if hasattr(self, method_name):
              method = getattr(self, method_name)
              result = method(cmd_args)
              if result:  # Command returned True (exit request)
                break
            else:
              print(f"Line {line_num}: Unknown command: {cmd_name}", file=sys.stderr)
          except Exception as e:
            print(f"Line {line_num}: Error executing '{line}': {e}", file=sys.stderr)
    except FileNotFoundError:
      print(f"File not found: {filename}", file=sys.stderr)
      sys.exit(1)
    except Exception as e:
      print(f"Error reading file {filename}: {e}", file=sys.stderr)
      sys.exit(1)

  #------------------------- Utility Functions
  def __get_frame_labels_str(self, frame_name: str) -> str:
    """Return CRUD labels for a frame as a single-line string, or empty string if none exist"""
    try:
      labels = self.__server.get_frame_labels(frame_name)
      has_labels = any(labels.values())
      if has_labels:
        acl_parts = []
        for crud_op in ['create', 'read', 'update', 'delete']:
          if labels[crud_op]:
            labels_str = ",".join(labels[crud_op])
            acl_parts.append(f"{crud_op}={labels_str}")
        return f"  [ACLs: {'; '.join(acl_parts)}]" if acl_parts else ""
    except Exception as e:
      if self.__debug:
        return f"  [Error retrieving labels: {e}]"
    return ""

  def __connect_to_server(self) -> xgt.Connection:
    """Establish a connection to the xGT server"""
    conn = None
    if self.__password:
      try:
        conn = xgt.Connection(port = self.__port,
                              host = self.__hostname,
                              auth = xgt.BasicAuth(
                                  username = self.__username,
                                  password = self.__password)
                             )
      except xgt.XgtError as exc:
        print(f"Unable to connect to xgtd server:\n{exc}")
    else:
      if self.__verbose:
        print(f"Trying to connect to server {self.__username}@{self.__hostname}:{self.__port}")
      try:
        conn = xgt.Connection(port = self.__port,
                              host = self.__hostname,
                              auth = xgt.BasicAuth(
                                  username = self.__username),
                             )
      except xgt.XgtError as exc:
        print(f"Unable to connect to xgtd server:\n{exc}")
    return conn

  def __process_job_command(self, line) -> None:
    fields = line.split()
    if len(fields) < 1 or len(fields) > 2:
      print(f"Command format: job <job_id> (<end-job-number>)")
      return None

    try:
      start_job = int(fields[0])
      if len(fields) > 1:
        end_job = int(fields[1])
      else:
        end_job = start_job
    except:
      print(f"Command format: job <job_id> (<end-job-number>)")
      return None

    jobs = self.__server.get_jobs()
    jobs_map = {_.id:_ for _ in jobs}
    for j in range(start_job, end_job+1):
      if j in jobs_map:
        job = jobs_map[j]
        print(f"Job #{job.id}, username: {job.user}, status {job.status}:")

#       if job.status == 'unknown_job_status':
#         return None

        if job.status == 'running':
          print(f"  start time: {job.start_time}")
        elif job.status != 'scheduled':
          duration = job.end_time - job.start_time
          print(f"    start time: {job.start_time}")
          print(f"      end time: {job.end_time}")
          print(f"      duration: {duration}")
        if len(job.description) > 0:
          print(f"   description: {job.description}")
        if 'query_plan' in dir(job) and len(job.query_plan) > 0:
          print(f"   query plan: {job.query_plan}")
        if job.visited_edges is not None and len(job.visited_edges) > 0:
          print(f" visited edges: {job.visited_edges}")
        if job.total_visited_edges is not None:
          print(f" total visited: {job.total_visited_edges}")
        if 'timing' in dir(job) and job.timing is not None and len(job.timing) > 0:
          print(f"        timing:")
          for line in job.timing:
            print(line)
        if '_timing' in dir(job) and job._timing is not None and len(job._timing) > 0:
          print(f"       _timing:")
          for line in job._timing:
            print(line)
        if job.schema is not None and len(job.schema) > 0:
          print(f"       schema: {job.schema}")
    return None

  def __version_is_since(self, major, minor, patch):
    (v1, v2, v3) = self.__version()
    if v1 > major:
      return True
    if v1 < major:
      return False
    if v2 > minor:
      return True
    if v2 < minor:
      return False
    return v3 >= patch

  def __version(self) -> (int):
    return (
        int(xgt.__version_major__),
        int(xgt.__version_minor__),
        int(xgt.__version_patch__),
        )

#----------------------------------------------------------------------------

if __name__ == '__main__' :
  name=os.path.basename(sys.argv[0])
  parser = argparse.ArgumentParser(
      prog=name,
      description='Command-line interface to Trovares xGT server')
  parser.add_argument('-d', '--debug', action='store_true',
      help="show debug information")
  parser.add_argument('--host', type=str, default='localhost',
      help="connect to this host name, default='localhost'")
  parser.add_argument('-p', '--port', type=int, default=4367,
      help='connect to this port, default=4367')
  parser.add_argument('-u', '--user', type=str,
      default=getpass.getuser(),
      help=f"username to use for the connection, default '{getpass.getuser()}'")
  parser.add_argument('--pw', '--password', type=str, dest='password',
      help="password to use for BasicAuth connection (optional)")
  parser.add_argument('-v', '--verbose', action='store_true',
      help="show detailed information")
  parser.add_argument('-c', '--command', type=str,
      help="execute a single command and exit")
  parser.add_argument('-q', '--query', type=str,
      help="execute a single Cypher query and exit")
  parser.add_argument('-f', '--file', type=str,
      help="execute commands from a file")
  parser.add_argument('-n', '--namespace', type=str,
      help="set the default namespace/dataset before executing commands")
  parser.add_argument('--format', type=str, choices=['table', 'json', 'csv'], default='table',
      help="output format for query results (default: table)")
  options = parser.parse_args(sys.argv[1:])

  instance = XgtCli(host=options.host, port=options.port, username=options.user,
                    password=options.password, verbose=options.verbose, debug=options.debug)

  # Handle non-interactive modes
  if options.query:
    instance.execute_query_and_exit(options.query, options.format, options.namespace)
  elif options.command:
    instance.execute_command_and_exit(options.command, options.namespace)
  elif options.file:
    instance.execute_file_and_exit(options.file)
  else:
    # For interactive mode, set the default namespace if specified
    if options.namespace:
      # Set namespace using the same approach as the do_default_namespace command
      instance.do_default_namespace(options.namespace)
      if options.verbose:
        print(f"Set default namespace to: {options.namespace}")
    instance.cmdloop()
