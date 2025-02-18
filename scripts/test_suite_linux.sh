#!/bin/bash

#       This script provies a comprehensive test suite of the built flows2fim executable on
#       linux systems, also including regression tests. This script, as well as testdata/reference_data
#       will need to be manually updated as features change. See testdata/readme.md for more information
#       on how to update the /testdata directory appropriately.

usage() {
    echo "
    This script tests the built flows2fim executable's methods with a combination of parameters.
    It must be run from the root directory of the flows2fim repository to ensure proper pathing for
    test data.

    Usage: ./scripts/test_suite_linux.sh [OPTIONS] [METHOD]

    OPTIONS:
        -d, --dev      Run tests using 'go run main.go' instead of compiled binary
        -h, --help     Show this help message

    METHODS:
        controls: Only issue the controls tests.
            e.g.: ./scripts/test_suite_linux.sh controls
        fim: Only issue the fim tests.
            e.g.: ./scripts/test_suite_linux.sh fim
        validate: Only issue the validate tests.
            e.g.: ./scripts/test_suite_linux.sh validate

    Providing no method, or 'all', will test all flows2fim methods, controls, fim, & validate.
    "
}

# Assign the command line argument if given, else 'all' is the default
dev_mode=false
cmd="flows2fim"
method="all"  # Default to 'all'

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        -d|--dev)
            dev_mode=true
            cmd="go run main.go"
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        -*)
            echo "Invalid option: $1"
            usage
            exit 1
            ;;
        *)
            method="$1"
            shift
            ;;
    esac
done

# Check if this script is being issued from the repo's root to ensure consistent filepaths
if [ "$(git rev-parse --show-toplevel)" != "$(pwd)" ]; then
    printf "Error: This script must be run from the root directory of the flows2fim repository."
    usage
    exit 1
fi

# Ensure flows2fim is installed, available, and executable
if $cmd --version > /dev/null; then
    printf "\n\t>>>> Testing availability of flows2fim executable. Output of flows2fim --version: <<<<\n\n"
    $cmd --version
else
    printf "flows2fim is not available"
    usage
    exit 1
fi

# Set paths and configurable variables
data_folder=testdata
ref_dir=$data_folder/reference_data
db_path=$ref_dir/db
controls_benchmark_dir=$ref_dir/controls
fim_benchmark_dir=$ref_dir/fim
flows_files_dir=$ref_dir/flows_files
library_benchmark=$ref_dir/library
start_reaches_dir=$ref_dir/start_reaches
fim_reference_output_formats=$ref_dir/fim_output_formats

test_outputs=$data_folder/test_out
control_test_outputs=$test_outputs/controls
fim_test_outputs=$test_outputs/fim
fim_test_output_formats=$test_outputs/fim_output_formats

# Set counter for failing tests
total_count=0
total_passed=0

test_help_statement() {
    printf "\n\t>>>> Test the --help usage statement <<<<\n\n"
    $cmd --help
}

exit_if_failure() {
    local failed=$1
    if [ "$failed" -gt 0 ]; then
        printf "\n\t>>>> ${failed} TESTS FAILED <<<<\n\n"
        exit 1
    else
        exit 0
    fi
}

compare_directories() {
    # Set args
    local dir1=$1
    local dir2=$2
    local fim=$3
    # Loop through files in dir1 and compare with dir2
    for file in "$dir1"/*; do
        filename=$(basename "$file")
        filepath2="$dir2/$filename"
        # Skip the one off .vrt relative test when comparing directories
        if [ "$filename" = "fim_2year_test_rel_false.vrt" ]; then
            continue
        fi
        # Check if the file exists in dir2, and if the "fim" argument was given
        if [ -f "$filepath2" ] && [ "$fim" = "fim" ]; then
            tempfile=$(mktemp)
            if [ "${filename:10:13}" = "vrt" ]; then
                temp_out=$(diff "$file" "$filepath2")
                if [ "$temp_out" != "" ]; then
                    printf "\t Output of diff: \n "$file" "$filepath2" \n $temp_out"
                    # printf "\t \u274c Files differ: $filename \n"
                fi
                continue
            fi
            gdalcompare.py $file $filepath2 &> "$tempfile"
            # cat "$tempfile"
            gdalcompare_output=$(tail -n 1 "$tempfile" | grep -Eo "[0-9]+" | tail -n 1)
            # echo "gdalcompare_output: $gdalcompare_output"
            # Remove temp file
            rm "$tempfile"
            # Set tolerance value of gdalcompare
            gdalcompare_difference_tolerance=2
            # If there are more than more differences than the tolerance value above, this test fails
            if (( $gdalcompare_output > $gdalcompare_difference_tolerance )); then
                printf "\n\t \u274c Files differ: $filename \n"
            fi
        elif [ -f "$filepath2" ]; then
            # Compare the files
            if ! cmp -s "$file" "$filepath2"; then
                temp_out=$(cmp "$file" "$filepath2")
                printf "Output of cmp -s "$file" "$filepath2" : \n\n $temp_out"
                printf "\t \u274c Files differ: $filename \n"
            fi
        else
            printf "\t \u274c File not found in dir2: $filename \n"
        fi
    done
    # Loop through files in dir2 to find any not in dir1
    for file in "$dir2"/*; do
        filename=$(basename "$file")
        filepath1="$dir1/$filename"
        # Check if the file does not exist in dir1
        if [ ! -f "$filepath1" ]; then
            printf "\t \u274c File in $dir2 not found in $dir1: $filename \n"
        fi
    done
}

# Define functions for each flows2fim method
controls_test_cases() {
    local num_test_cases_controls=9
    local failed_controls_testcases=0
    total_count=$(( total_count + num_test_cases_controls))
    # If previous directory exists, remove it
    if [ -d "$control_test_outputs" ]; then
        rm -rf "$control_test_outputs"
    fi
    # Create new test output directory
    mkdir -p $control_test_outputs

    printf "\n\t###### ${num_test_cases_controls} TEST CASES FOR flows2fim controls  ######\n\n"

    # Define the recurrence interval array
    local recurrence_interval=(2 5 10 25 50 100)
    printf "(1/${num_test_cases_controls})\t>>>> Generate controls.csv files from "
    printf "${recurrence_interval[*]} year recurrence interval data for Regression Testing. <<<<\n\n"

        for interval in "${recurrence_interval[@]}"; do
            # Execute flows2fim with each recurrence interval
            $cmd controls -db $db_path/ripple.gpkg \
                -f $flows_files_dir/flows_${interval}year.csv \
                -o $control_test_outputs/controls_${interval}year.csv \
                -scsv $start_reaches_dir/start_reaches.csv
        done

    printf "\n(2/${num_test_cases_controls})\t>>>> Regression Tests for all recur. int. controls files <<<<\n\n"
        # Compare controls files from recently generated test data and benchmark data
        # Capture the output of the comparison as a variable if there is no difference
        diff_output=$(compare_directories "$control_test_outputs" "$controls_benchmark_dir")

        if [ -z "$diff_output" ]; then
            printf "\t \u2714 No difference in controls files. \n\n"
        else
            printf "\t \u274c Outputs differ: \n"
            printf "\t $diff_output \n"
            failed_controls_testcases=$((failed_controls_testcases + 1))
        fi

    ## Assert correct errors thrown
    printf "(3/${num_test_cases_controls})\t>>>> Assert Error thrown from no start reaches parameter <<<<\n\n"
        # Create and assign temp file
        tempfile=$(mktemp)
        # Test case
        $cmd controls -db $db_path/ripple.gpkg \
            -f $flows_files_dir/flows_2year.csv \
            -o $control_test_outputs/controls_2year.csv &> "$tempfile"
        # Here we use head -n 1 to capture the first line redirected to the tmp file
        first_line=$(head -n 1 "$tempfile")
        # Remove temp file
        rm "$tempfile"
        # Assign error string
        assert_expected_output_1="Error: either a CSV file or start reach IDs and control stages must be provided"
        # Compare Error messaging and print
        if [ "$first_line" = "$assert_expected_output_1" ]; then
            printf "\t \u2714 Correct error thrown. \n\n"
        else
            printf "\t \u274c Error messaging inconsistent \n\n"
            failed_controls_testcases=$((failed_controls_testcases + 1))
        fi

    printf "(4/${num_test_cases_controls})\t>>>> Assert Error thrown from no start reaches parameter, or output parameter <<<<\n\n"
        # Create and assign temp file
        tempfile=$(mktemp)
        # Test case
        $cmd controls -db $db_path/ripple.gpkg -f $flows_files_dir/flows_2year.csv &> "$tempfile"
        # Here we use head -n 1 to capture the first line redirected to the tmp file
        first_line=$(head -n 1 "$tempfile")
        # Remove temp file
        rm "$tempfile"
        # Assign error string
        assert_expected_output_2="Missing required flags"
        # Compare Error messaging and print
        if [ "$first_line" = "$assert_expected_output_2" ]; then
            printf "\t \u2714 Correct error thrown. \n\n"
        else
            printf "\t \u274c Error messaging inconsistent \n\n"
            failed_controls_testcases=$((failed_controls_testcases + 1))
        fi

    printf "(5/${num_test_cases_controls})\t>>>> Assert Error thrown from only providing db parameter <<<<\n\n"
        # Create and assign temp file
        tempfile=$(mktemp)
        # Test case
        $cmd controls -db $db_path/ripple.gpkg &> "$tempfile"
        # Here we use head -n 1 to capture the first line redirected to the tmp file
        first_line=$(head -n 1 "$tempfile")
        # Remove temp file
        rm "$tempfile"
        # Assign error string
        assert_expected_output_3="Missing required flags"
        # Compare Error messaging and print
        if [ "$first_line" = "$assert_expected_output_3" ]; then
            printf "\t \u2714 Correct error thrown. \n\n"
        else
            printf "\t \u274c Error messaging inconsistent. \n\n"
            failed_controls_testcases=$((failed_controls_testcases + 1))
        fi

    ## Test empty controls file created under different flows file and start reaches file inputs
    printf "(6/${num_test_cases_controls})\t>>>> If start reaches file is empty, confirm controls file is empty <<<<\n\n"
        # Create and assign temp file
        tempfile=$(mktemp)
        # Test case
        $cmd controls -db $db_path/ripple.gpkg \
            -f $flows_files_dir/flows_2year.csv \
            -o $tempfile \
            -scsv $start_reaches_dir/empty_start_reaches.csv &> /dev/null
        # Here we use head -n 2 to capture the first two line of the tmp file
        file_contents=$(head -n 2 "$tempfile")
        # Remove temp file
        rm "$tempfile"
        # Assign error string
        assert_file_output="reach_id,flow,control_stage"
        # Compare Error messaging and print
        if [ "$file_contents" = "$assert_file_output" ]; then
            printf "\t \u2714 Passed: Output file created and empty. \n\n"
        else
            printf "\t \u274c Failed: Output file not empty \n\n"
            failed_controls_testcases=$((failed_controls_testcases + 1))
        fi

    printf "(7/${num_test_cases_controls})\t>>>> If flows file is empty, confirm Flow not found error thrown <<<<\n\n"
        # Create and assign temp file
        tempfile=$(mktemp)
        temp_out=$(mktemp)
        # Test case
        $cmd controls -db $db_path/ripple.gpkg \
            -f $flows_files_dir/empty_file.csv \
            -o $temp_out \
            -scsv $start_reaches_dir/start_reaches.csv &> "$tempfile"
        # Assign error string
        assert_error_message="Flow not found for reach 24274737"
        file_contents=$(grep -n "$assert_error_message" "$tempfile" | cut -c 1)
        # Remove temp files
        rm "$temp_out"
        rm "$tempfile"
        # Compare output of grep (1 indicates $assert_error_message was found in stdout redirected to file)
        if [ "$file_contents" = 1 ]; then
            printf "\t \u2714 Passed: Flow not found error thrown. \n\n"
        else
            printf "\t \u274c Failed: Flow not found error not thrown \n\n"
            failed_controls_testcases=$((failed_controls_testcases + 1))
        fi

    printf "(8/${num_test_cases_controls})\t>>>> If flows file's columns are swapped, confirm Flow not found error thrown <<<<\n\n"
        # Create and assign temp file
        tempfile=$(mktemp)
        temp_out=$(mktemp)
        # Test case
        $cmd controls -db $db_path/ripple.gpkg \
            -f $flows_files_dir/columns_swapped.csv \
            -o $temp_out \
            -scsv $start_reaches_dir/start_reaches.csv &> "$tempfile"
        # Assign error string
        assert_error_message="Flow not found for reach 24274737"
        file_contents=$(grep -n "$assert_error_message" "$tempfile" | cut -c 1)
        # Remove temp files
        rm "$temp_out"
        rm "$tempfile"
        # Compare output of grep (1 indicates $assert_error_message was found in stdout redirected to file)
        if [ "$file_contents" = 1 ]; then
            printf "\t \u2714 Passed: Flow not found error thrown. \n\n"
        else
            printf "\t \u274c Failed: Flow not found error not thrown \n\n"
            failed_controls_testcases=$((failed_controls_testcases + 1))
        fi

    printf "(9/${num_test_cases_controls})\t>>>> If flows file's values are empty, confirm Flow not found error thrown <<<<\n\n"
        # Create and assign temp file
        tempfile=$(mktemp)
        temp_out=$(mktemp)
        # Test case
        $cmd controls -db $db_path/ripple.gpkg \
            -f $flows_files_dir/empty_flow_values_no_header.csv \
            -o $temp_out \
            -scsv $start_reaches_dir/start_reaches.csv &> "$tempfile"

        # Assign error string
        assert_error_message="Flow not found for reach 24274737"
        file_contents=$(grep -n "$assert_error_message" "$tempfile" | cut -c 1)
        # Remove temp files
        rm "$temp_out"
        rm "$tempfile"
        # Compare output of grep (1 indicates $assert_error_message was found in stdout redirected to file)
        if [ "$file_contents" = 1 ]; then
            printf "\t \u2714 Passed: Flow not found error thrown. \n\n"
        else
            printf "\t \u274c Failed: Flow not found error not thrown \n\n"
            failed_controls_testcases=$((failed_controls_testcases + 1))
        fi

    controls_passed=$((num_test_cases_controls - failed_controls_testcases))
    total_passed=$(( total_passed + controls_passed))
}

fim_test_cases() {
    local num_test_cases_fim=9
    local failed_fim_testcases=0
    total_count=$(( total_count + num_test_cases_fim))
    # If previous directories exists, remove them
    if [ -d "$fim_test_outputs" ]; then
        rm -rf "$fim_test_outputs"
        rm -rf "$fim_test_output_formats"
    fi
    # Create new test output directories
    mkdir -p $fim_test_outputs
    mkdir -p $fim_test_output_formats

    printf "\n\t###### ${num_test_cases_fim} TEST CASES FOR flows2fim fim  ######\n\n"
    printf "\n\t>>>> Test flows2fim fim <<<<\n\n"

    # Define the recurrence interval array
    local recurrence_interval=(2 5 10 25 50 100)
    printf "(1/${num_test_cases_fim})\t>>>> Generate fim_year.tif files from "
    printf "${recurrence_interval[*]} year recurrence interval data for Regression Testing. <<<<\n\n"
        local fim_file_format="tif"
        for interval in "${recurrence_interval[@]}"; do
            # Execute $cmd with each recurrence interval
            $cmd fim \
                -c $controls_benchmark_dir/controls_${interval}year.csv \
                -fmt $fim_file_format \
                -lib $library_benchmark \
                -o $fim_test_outputs/fim_${interval}year.tif &> /dev/null
        done

    printf "(2/${num_test_cases_fim})\t>>>> Regression Tests for all recur. int. fim.tif files <<<<\n\n"
        # Compare fim files from recently generated test data and benchmark data
        # Capture the output of the compare_directories function as a variable
        diff_output=$(compare_directories "$fim_test_outputs" "$fim_benchmark_dir" "fim")

        # If there is no differnce, test pass
        if [ -z "$diff_output" ]; then
            printf "\t \u2714 No significant difference in fim.tif files. \n\n"
        else
            printf "\t \u274c Outputs differ: \n\n"
            printf "\t $diff_output \n"
            failed_fim_testcases=$((failed_fim_testcases + 1))
        fi

    printf "(3/${num_test_cases_fim})\t>>>> Generate fim files in different output formats <<<<\n\n"
        local file_formats=( "tif" "cog" "vrt" )

        for format in "${file_formats[@]}"; do
            local output_file=fim_2year.$format
            # Execute flows2fim with each file format (redirecting stdout to tidy logging)
            $cmd fim \
                -c $controls_benchmark_dir/controls_2year.csv \
                -fmt $format \
                -lib $library_benchmark \
                -o $fim_test_output_formats/$output_file \
                -rel false &> /dev/null
        done

    printf "(4/${num_test_cases_fim})\t>>>> Regression Tests for different output formats <<<<\n\n"
        # Note:
        # The vrt file creation with -rel true (default behavior) is a failing case,
        # because the vrt contains different values for the "relativeToVRT". This is due to
        # the behavior of the -rel flag to flows2fim fim not working as expected.
        # Capture the output of the comparison function as a variable
        diff_output=$(compare_directories "$fim_reference_output_formats" "$fim_test_output_formats" "fim")
        # If there is no difference between directories, test pass
        if [ -z "$diff_output" ]; then
            printf "\t \u2714 No differences in .cog, .vrt & .tif files. \n\n"
        else
            printf "\t \u274c Outputs differ: \n\n"
            printf "$diff_output \n"
            failed_fim_testcases=$((failed_fim_testcases + 1))
        fi

    printf "(5/${num_test_cases_fim})\t>>>> Assert flows2fim fim -rel argument creates .vrt with correct xml output <<<<\n\n"
        output_file=fim_2year_test_rel_false.vrt
        $cmd fim \
            -c $controls_benchmark_dir/controls_2year.csv \
            -fmt "vrt" \
            -lib $library_benchmark \
            -o $fim_test_output_formats/$output_file \
            -rel false &> /dev/null

        diff_output=$(diff "$fim_reference_output_formats/$output_file" "$fim_test_output_formats/$output_file")
        # If there is no differnce between files, test pass
        if [ -z "$diff_output" ]; then
            printf "\t \u2714 No differences in .vrt files. \n\n"
        else
            printf "\t \u274c Outputs differ: \n\n"
            printf "$diff_output \n"
            failed_fim_testcases=$((failed_fim_testcases + 1))
        fi

    printf "(6/${num_test_cases_fim})\t>>>> Assert Error thrown from no controls file parameter <<<<\n\n"
        # Create and assign temp file
        tempfile=$(mktemp)
        # Test case
        $cmd fim \
                -fmt $format \
                -lib $library_benchmark \
                -o $fim_test_outputs/fim_test.tif &> "$tempfile"
        # Here we use sed -n 2 p to capture the second line redirected to the tmp file
        first_line=$(cat "$tempfile" | sed -n '2 p')
        # Remove temp file
        rm "$tempfile"
        # Assign error string
        assert_expected_output_1="Missing required flags"
        # Compare Error messaging and print
        if [ "$first_line" = "$assert_expected_output_1" ]; then
            printf "\t \u2714 Correct error thrown. \n\n"
        else
            printf "\t \u274c Error messaging inconsistent \n\n"
            failed_fim_testcases=$((failed_fim_testcases + 1))
        fi

    printf "(7/${num_test_cases_fim})\t>>>> Assert Error thrown from no library parameter <<<<\n\n"
        # Create and assign temp file
        tempfile=$(mktemp)
        # Test case
        $cmd fim \
                -c $controls_benchmark_dir/controls_2year.csv \
                -fmt "tif" \
                -o $fim_test_outputs/fim_test.tif &> "$tempfile"
        # Here we use head -n 1 to capture the first line redirected to the tmp file
        first_line=$(tail -n 1 "$tempfile")
        # Remove temp file
        rm "$tempfile"
        # Assign error string
        assert_expected_output_1="Error: error running gdalwarp: exit status 2"
        # Compare Error messaging and print
        if [ "$first_line" = "$assert_expected_output_1" ]; then
            printf "\t \u2714 Correct error thrown. \n\n"
        else
            printf "\t \u274c Error messaging inconsistent \n\n"
            failed_fim_testcases=$((failed_fim_testcases + 1))
        fi

    printf "(8/${num_test_cases_fim})\t>>>> Assert Error thrown from no output file parameter <<<<\n\n"
        # Create and assign temp file
        tempfile=$(mktemp)
        # Test case
        $cmd fim \
                -c $controls_benchmark_dir/controls_2year.csv \
                -fmt $format \
                -lib $library_benchmark &> "$tempfile"
        # Here we use sed -n 2 p to capture the second line redirected to the tmp file
        first_line=$(cat "$tempfile" | sed -n '2 p')
        # Remove temp file
        rm "$tempfile"
        # Assign error string
        assert_expected_output_1="Missing required flags"
        # Compare Error messaging and print
        if [ "$first_line" = "$assert_expected_output_1" ]; then
            printf "\t \u2714 Correct error thrown. \n\n"
        else
            printf "\t \u274c Error messaging inconsistent \n\n"
            failed_fim_testcases=$((failed_fim_testcases + 1))
        fi

    printf "(9/${num_test_cases_fim})\t>>>> Assert Error thrown from empty controls file <<<<\n\n"
        # Create and assign temp file
        tempfile=$(mktemp)
        # Test case
        $cmd fim \
                -c $flows_files_dir/empty_file.csv \
                -fmt $format \
                -lib $library_benchmark \
                -o $fim_test_outputs/fim_test.tif &> "$tempfile"
        # Here we use head -n 1 to capture the first line redirected to the tmp file
        first_line=$(head -n 1 "$tempfile")
        # Remove temp file
        rm "$tempfile"
        # Assign error string
        assert_expected_output_1="Error: no records in control file"
        # Compare Error messaging and print
        if [ "$first_line" = "$assert_expected_output_1" ]; then
            printf "\t \u2714 Correct error thrown. \n\n"
        else
            printf "\t \u274c Error messaging inconsistent \n\n"
            failed_fim_testcases=$((failed_fim_testcases + 1))
        fi

    #printf "(10/${num_test_cases_fim})\t>>>> Test flows2fim fim pull from S3 <<<<\n\n"

    fim_passed=$((num_test_cases_fim - failed_fim_testcases))
    total_passed=$(( total_passed + fim_passed))

}

validate_test_cases() {
    local num_test_cases_validate=0
    local failed_validate_testcases=0
    total_count=$(( total_count + num_test_cases_validate))

    printf "\n\t###### ${num_test_cases_validate} TEST CASES FOR flows2fim validate  ######\n\n"
    # printf "( 1/${num_test_cases_validate})\t>>>> Test validate <<<<\n\n"
    # flows2fim validate


    validate_passed=$((num_test_cases_validate - failed_validate_testcases))
    total_passed=$((total_passed + validate_passed))
}

# Control flow for methods and test cases
if [ "$method" = "all" ]; then
    printf "\n\t>>>> TESTING ALL METHODS (controls, fim, & validate) <<<<\n"
    test_help_statement
    controls_test_cases
    fim_test_cases
    validate_test_cases
    printf "\n\t>>>> (${total_passed}/${total_count}) TESTS PASSED <<<<\n\n"
    total_failed=$((total_count - total_passed))
    exit_if_failure $total_failed
elif [ "$method" = "controls" ]; then
    controls_test_cases
    printf "\n\t>>>> (${total_passed}/${total_count}) TESTS PASSED <<<<\n\n"
    total_failed=$((total_count - total_passed))
    exit_if_failure $total_failed
elif [ "$method" = "fim" ]; then
    fim_test_cases
    printf "\n\t>>>> (${total_passed}/${total_count}) TESTS PASSED <<<<\n\n"
    total_failed=$((total_count - total_passed))
    exit_if_failure $total_failed
elif [ "$method" = "validate" ]; then
    validate_test_cases
    printf "\n\t>>>> (${total_passed}/${total_count}) TESTS PASSED <<<<\n\n"
    total_failed=$((total_count - total_passed))
    exit_if_failure $total_failed
else
    printf "\n\t>>>> NO TEST CASES WERE RUN <<<<\n\n"
    usage
fi