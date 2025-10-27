$(document).ready(function() {
    let progressInterval;
    let logsInterval;
    let resultsInterval;

    function getProgress() {
        $.get("/progress", function(data) {
            let progressBar = $("#progress-bar");
            let progress = data.progress || 0;
            progressBar.css("width", progress + "%");
            progressBar.text(progress + "%");
            progressBar.attr("aria-valuenow", progress);

            if (progress >= 100) {
                clearInterval(progressInterval);
                clearInterval(logsInterval);
                clearInterval(resultsInterval);
                $("#export-sheet").prop("disabled", false);
            }
        });
    }

    function getLogs() {
        $.get("/logs", function(data) {
            let logs = $("#logs");
            logs.text(data);
            logs.scrollTop(logs[0].scrollHeight);
        });
    }

    function getResults() {
        $.get("/results", function(data) {
            let tableBody = $("#results-table-body");
            tableBody.empty();
            data.forEach(function(result) {
                let row = `<tr>
                    <td>${result['Meeting Folder']}</td>
                    <td>${result['RP Number']}</td>
                    <td>${result['R4 Document']}</td>
                    <td>${result['Matching Clause']}</td>
                    <td>${result['Summary of Change']}</td>
                </tr>`;
                tableBody.append(row);
            });
        });
    }

    $("#start-tracking").click(function() {
        let specNumber = $("#spec-number").val();
        if (!specNumber) {
            alert("Please enter a spec number.");
            return;
        }

        $(".progress-container").show();
        $("#logs-container").show();
        $("#results-container").show();

        $.ajax({
            url: "/start-tracking",
            type: "POST",
            contentType: "application/json",
            data: JSON.stringify({ spec_number: specNumber }),
            success: function(response) {
                console.log(response.message);
                progressInterval = setInterval(getProgress, 1000);
                logsInterval = setInterval(getLogs, 1000);
                resultsInterval = setInterval(getResults, 1000);
            },
            error: function(xhr, status, error) {
                alert("Error starting tracking: " + xhr.responseText);
            }
        });
    });

    $("#export-sheet").click(function() {
        var wb = XLSX.utils.table_to_book(document.querySelector('#results-container .table-bordered'), {sheet:"Results"});
        XLSX.writeFile(wb, "3gpp_results.xlsx");
    });
});
