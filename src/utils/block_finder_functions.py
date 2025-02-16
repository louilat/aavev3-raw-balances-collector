"""Function to find closest block to timestamp"""


def find_closest_block(
    w3, target_timestamp: int, initial_block: int, step: int, verbose: bool = False
) -> int:
    upper_bound = initial_block
    lower_bound = initial_block - step
    lower_timestamp = w3.eth.get_block(lower_bound)["timestamp"]

    while lower_timestamp > target_timestamp:
        lower_bound -= step
        upper_bound -= step
        lower_timestamp = w3.eth.get_block(lower_bound)["timestamp"]

    assert w3.eth.get_block(lower_bound)["timestamp"] <= target_timestamp
    assert w3.eth.get_block(upper_bound)["timestamp"] > target_timestamp

    # Starting Dichotomy
    iteration = 0
    while abs(upper_bound - lower_bound) > 1:
        iteration += 1
        mid_bound = round((lower_bound + upper_bound) / 2)
        mid_timestamp = w3.eth.get_block(mid_bound)["timestamp"]

        if mid_timestamp < target_timestamp:
            lower_bound = mid_bound
        else:
            upper_bound = mid_bound

        if verbose and iteration % 10 == 0:
            print(f"Iteration {iteration}")
            print("   -->lower = ", lower_bound)
            print("   -->upper = ", upper_bound)

    return lower_bound
