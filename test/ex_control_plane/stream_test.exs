defmodule ExControlPlane.StreamTest do
  use ExUnit.Case, async: false

  setup do
    Application.ensure_all_started(:ex_control_plane)

    {:ok, pid} = DynamicSupervisor.start_link(name: ExControlPlane.DynamicTestSupervisor)

    on_exit(fn ->
      Application.stop(:ex_control_plane)
      Application.unload(:ex_control_plane)
      Process.exit(pid, :kill)
    end)
  end

  test "grpc stream is killed" do
    assert 0 == Registry.count(ExControlPlane.StreamRegistry)

    assert %{active: 0, workers: 0, supervisors: 0, specs: 0} ==
             DynamicSupervisor.count_children(ExControlPlane.StreamSupervisor)

    {:ok, grpc_stream_pid} =
      DynamicSupervisor.start_child(ExControlPlane.DynamicTestSupervisor, {MockGRPCStream, []})

    grpc_stream = %{payload: %{pid: grpc_stream_pid}}
    node_info = %{cluster: "cluster", node_id: "id"}
    type_url = "type.googleapis.com/envoy.config.route.v3.ScopedRouteConfiguration"

    {:ok, stream_pid} = ExControlPlane.Stream.ensure_registred(grpc_stream, node_info, type_url)

    assert 1 == Registry.count(ExControlPlane.StreamRegistry)

    assert %{active: 1, workers: 1, supervisors: 0, specs: 1} ==
             DynamicSupervisor.count_children(ExControlPlane.StreamSupervisor)

    # kill grpc stream
    true = Process.exit(grpc_stream_pid, :kill)
    # should send a DOWN message to the Genserver which should then :stop
    assert wait(fn -> not Process.alive?(stream_pid) end, 1000)

    # and deregister/be removed from dynamic supervisor
    assert %{active: 0, workers: 0, supervisors: 0, specs: 0} ==
             DynamicSupervisor.count_children(ExControlPlane.StreamSupervisor)

    assert 0 == Registry.count(ExControlPlane.StreamRegistry)
  end

  test "ExControlPlane.Stream is killed (not GRPC stream itself)" do
    assert 0 == Registry.count(ExControlPlane.StreamRegistry)

    assert %{active: 0, workers: 0, supervisors: 0, specs: 0} ==
             DynamicSupervisor.count_children(ExControlPlane.StreamSupervisor)

    {:ok, grpc_stream_pid} =
      DynamicSupervisor.start_child(ExControlPlane.DynamicTestSupervisor, {MockGRPCStream, []})

    grpc_stream = %{payload: %{pid: grpc_stream_pid}}
    node_info = %{cluster: "cluster", node_id: "id"}
    type_url = "type.googleapis.com/envoy.config.route.v3.ScopedRouteConfiguration"

    {:ok, stream_pid} = ExControlPlane.Stream.ensure_registred(grpc_stream, node_info, type_url)

    assert 1 == Registry.count(ExControlPlane.StreamRegistry)

    assert %{active: 1, workers: 1, supervisors: 0, specs: 1} ==
             DynamicSupervisor.count_children(ExControlPlane.StreamSupervisor)

    assert [{_, ^stream_pid, :worker, [ExControlPlane.Stream]}] =
             DynamicSupervisor.which_children(ExControlPlane.StreamSupervisor)

    assert [^stream_pid] =
             Registry.select(ExControlPlane.StreamRegistry, [
               {{{:_, :"$1", :"$2"}, :"$3", :_},
                [{:==, :"$1", node_info.cluster}, {:==, :"$2", type_url}], [:"$3"]}
             ])

    assert %{stream: %{payload: %{pid: ^grpc_stream_pid}}} =
             GenServer.call(stream_pid, :test)

    # kill ExControlPlane.Stream
    true = Process.exit(stream_pid, :kill)
    assert wait(fn -> not Process.alive?(stream_pid) end, 1000)

    # should restart it
    assert wait(
             fn ->
               %{active: 1, workers: 1, supervisors: 0, specs: 1} ==
                 DynamicSupervisor.count_children(ExControlPlane.StreamSupervisor)
             end,
             1000
           )

    [{_, new_stream_pid, :worker, [ExControlPlane.Stream]}] =
      DynamicSupervisor.which_children(ExControlPlane.StreamSupervisor)

    # ExControlPlane has been restarted and assigned a new PID
    assert new_stream_pid != stream_pid
    assert 1 == Registry.count(ExControlPlane.StreamRegistry)

    # registry has been updated and contains only the new PID
    assert [^new_stream_pid] =
             Registry.select(ExControlPlane.StreamRegistry, [
               {{{:_, :"$1", :"$2"}, :"$3", :_},
                [{:==, :"$1", node_info.cluster}, {:==, :"$2", type_url}], [:"$3"]}
             ])

    # Stream has been restarted and contains correct (unchanged) GRPC Stream PID
    assert %{stream: %{payload: %{pid: ^grpc_stream_pid}}} =
             GenServer.call(new_stream_pid, :test)
  end

  test "two Envoys" do
    assert 0 == Registry.count(ExControlPlane.StreamRegistry)

    assert %{active: 0, workers: 0, supervisors: 0, specs: 0} ==
             DynamicSupervisor.count_children(ExControlPlane.StreamSupervisor)

    {:ok, grpc_stream_pid} =
      DynamicSupervisor.start_child(ExControlPlane.DynamicTestSupervisor, {MockGRPCStream, []})

    {:ok, grpc_stream_pid2} =
      DynamicSupervisor.start_child(ExControlPlane.DynamicTestSupervisor, {MockGRPCStream, []})

    grpc_stream = %{payload: %{pid: grpc_stream_pid}}
    grpc_stream2 = %{payload: %{pid: grpc_stream_pid2}}
    node_info = %{cluster: "cluster", node_id: "id"}
    type_url = "type.googleapis.com/envoy.config.route.v3.ScopedRouteConfiguration"

    {:ok, stream_pid} = ExControlPlane.Stream.ensure_registred(grpc_stream, node_info, type_url)
    {:ok, stream_pid2} = ExControlPlane.Stream.ensure_registred(grpc_stream2, node_info, type_url)

    assert 2 == Registry.count(ExControlPlane.StreamRegistry)

    assert %{active: 2, workers: 2, supervisors: 0, specs: 2} ==
             DynamicSupervisor.count_children(ExControlPlane.StreamSupervisor)

    stream_pid_list = [stream_pid, stream_pid2] |> Enum.sort()

    assert [
             {_, ^stream_pid, :worker, [ExControlPlane.Stream]},
             {_, ^stream_pid2, :worker, [ExControlPlane.Stream]}
           ] =
             DynamicSupervisor.which_children(ExControlPlane.StreamSupervisor)

    assert stream_pid_list ==
             Registry.select(ExControlPlane.StreamRegistry, [
               {{{:_, :"$1", :"$2"}, :"$3", :_},
                [{:==, :"$1", node_info.cluster}, {:==, :"$2", type_url}], [:"$3"]}
             ])
             |> Enum.sort()

    assert %{stream: %{payload: %{pid: ^grpc_stream_pid}}} =
             GenServer.call(stream_pid, :test)

    assert %{stream: %{payload: %{pid: ^grpc_stream_pid2}}} =
             GenServer.call(stream_pid2, :test)
  end

  defp wait(f, until) when until > 0 do
    if f.() do
      :ok
    else
      Process.sleep(100)
      wait(f, until - 100)
    end
  end
end

defmodule MockGRPCStream do
  use Agent

  def start_link(_opts) do
    Agent.start_link(fn -> %{} end)
  end
end
