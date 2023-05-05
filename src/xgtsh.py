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
import sys

import xgt

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

  def __init__(self, host, port, userid):
    super().__init__()

    self.__userid = userid
    self.__port = port
    self.__hostname = host
    self.__server = self.__connect_to_server()
    self.__verbose = False

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
    if READLINE_DEFINED:
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
    """Show detail information on a job"""
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

  def do_query(self, line)->bool:
    """Run a query"""
    if self.__server is None:
      print("Not connected to a server")
    else:
      job = self.__server.run_job(line)
      df = job.get_data_pandas()
      print(df)
    return False

  def do_schema(self, line)->bool:
    """Show schema of specified frame"""
    frame_name = line
    print(f"Showing schema of frame {frame_name}")
    is_edge = False
    try:
      frame = self.__server.get_edge_frame(frame_name)
      is_edge = True
    except:
      try:
        frame = self.__server.get_vertex_frame(frame_name)
      except:
        try:
          frame = self.__server.get_table_frame(frame_name)
        except:
          print(f"Frame {frame_name} does not exist")
          return False
    schema = frame.schema
    print(f"Schema: {schema}")
    if is_edge:
      print(f"Source frame: {frame.source_name}, Target frame: {frame.target_name}")
    return False

  def do_scroll(self, line)->bool:
    """Scroll through frame data"""
    frame_name = line
    try:
      frame = self.__server.get_edge_frame(frame_name)
    except:
      try:
        frame = self.__server.get_vertex_frame(frame_name)
      except:
        try:
          frame = self.__server.get_table_frame(frame_name)
        except:
          print(f"Frame {frame_name} does not exist")
          return False
    offset = 0
    data = frame.get_data(offset, 20)
    print(f"Data:\n{data}")
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

    tables = self.__server.get_table_frames(namespace=ns)
    total_table_rows = 0
    for table in tables:
      if self.__verbose or not table.name.startswith('xgt__'):
        total_table_rows += table.num_rows
        print(f"TableFrame {table.name} has {table.num_rows:,} rows")
    print(f"Total table rows over all frames: {total_table_rows:,}")
    vertices = self.__server.get_vertex_frames(namespace=ns)
    total_vertices = 0
    for vertex in vertices:
      if self.__verbose or not vertex.name.startswith('xgt__'):
        total_vertices += vertex.num_rows
        print(f"VertexFrame {vertex.name} has {vertex.num_vertices:,} vertices")
    print(f"Total vertices over all frames: {total_vertices:,}")
    edges = self.__server.get_edge_frames(namespace=ns)
    total_edges = 0
    for edge in edges:
      if self.__verbose or not edge.name.startswith('xgt__'):
        total_edges += edge.num_edges
        print(f"EdgeFrame {edge.name} has {edge.num_edges:,} edges")
    print(f"Total edges over all frames: {total_edges:,}")

    return False
  complete_show = _namespace_complete

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
    if len(tables) + len(vertices) + len(edges) == 0:
      print(f"No frames in namespace {ns} found")
    return False
  complete_zap = _namespace_complete

  #------------------------- Utility Functions
  def __connect_to_server(self, password=None) -> xgt.Connection:
    """Establish a connection to the xGT server"""
    conn = None
    if password:
      try:
        conn = xgt.Connection(port = self.__port,
                              host = self.__hostname,
                              userid = self.__userid,
                              credentials = getpass.getpass())
      except xgt.XgtError as exc:
        print(f"Unable to connect to xgtd server:\n{exc}")
    else:
      try:
        conn = xgt.Connection(port = self.__port,
                              host = self.__hostname,
                              userid = self.__userid)
      except xgt.XgtError as exc:
        print(f"Unable to connect to xgtd server:\n{exc}")
    return conn

  def __process_job_command(self, line) -> None:
    fields = line.split()
    if len(fields) != 1:
      print(f"Command format: job <job_id>")
      return None
    try:
      job_id = int(fields[0])
    except:
      print(f"Error: job_id must be an integer")
      return None
    job = self.__server.get_jobs([job_id])[0]
    print(f"Job #{job.id}, userid: {job.user}, status {job.status}:")
    if job.status == 'unknown_job_status':
      return None

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

#----------------------------------------------------------------------------

if __name__ == '__main__' :
  name=os.path.basename(sys.argv[0])
  parser = argparse.ArgumentParser(
      prog=name,
      description='Command-line interface to Trovares xGT server')
  parser.add_argument('--host', type=str, default='localhost',
      help="connect to this host name, default='localhost'")
  parser.add_argument('-p', '--port', type=int, default=4367,
      help='connect to this port, default=4367')
  parser.add_argument('-u', '--user', type=str,
      default=getpass.getuser(),
      help=f"userid to use for the connection, default '{getpass.getuser()}'")
  parser.add_argument('-v', '--verbose', action='store_true',
      help="show detailed information")
  options = parser.parse_args(sys.argv[1:])

  instance = XgtCli(host=options.host, port=options.port, userid=options.user)
  instance.cmdloop()
