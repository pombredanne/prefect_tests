import argparse

from prefect.engine import FlowRunner

from flows.dataframe_flows import simple_df_flow, simple_csv_df_flow
from flows.docker_flows import images_up_flow, containers_down_flow

# This should be in a database or in some sort of
# config file
FLOW_SET = {
    '0f12': (simple_df_flow, dict(initial_data=dict(column_c=[1, 2, 3, 4]))),
    '7fr2': (simple_csv_df_flow, dict(file_path='../mock_data.csv', separator='|')),
    'a332': (images_up_flow, dict(repository='mongo', image_tag='latest', amount=3)),
    '11gf': (containers_down_flow, dict())
}


if __name__ == '__main__':
    # You need to adjust the run config in pycharm and add the flow_id parameter
    # if you run this from the console just pass the flow_id
    parser = argparse.ArgumentParser(description='Run a flow by its flow id')
    parser.add_argument('flow_id', type=str)

    args = parser.parse_args()

    flow_to_run = FLOW_SET.get(args.flow_id, None)

    if flow_to_run:
        print(f'Launching "{args.flow_id}" flow...')
        fr = FlowRunner(flow=flow_to_run[0])
        # Parameters of course will come from a different location, this is just
        # to illustrate how one would call any given flow with their parameters
        flow_state = fr.run(parameters=flow_to_run[1])
    else:
        print(f'The flow_id "{args.flow_id}" does not match any flow in the system!')
