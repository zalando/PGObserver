var defaultLabelWidth = 70;
var $navcontainer;
var $topbar;
var $grids;
var $gridclear4;
var $gridclear6;
var $topsearch;
var $navlinks;
var $flots = [];
var delayedResize;

function ShowDatabasesMenu()
{
    $topbar.hide();
    $navcontainer.show();
}

function HideDatabasesMenu()
{
    $navcontainer.hide();
    $topbar.show();
}

function CheckGridSize()
{
    var w = $($grids.get(0)).outerWidth();

    if (w < 550) {
        $grids.addClass("grid_6").removeClass("grid_4");
        $gridclear4.hide();
        $gridclear6.show();
    } else {
        $grids.addClass("grid_4").removeClass("grid_6");
        $gridclear6.hide();
        $gridclear4.show();
    }

    if (delayedResize) {
        window.clearTimeout(delayedResize);
        delayedResize = false;
    }

    delayedResize = setTimeout(function() {
        $.each($flots, function(i, r) {
            r.resize();
            r.setupGrid();
            r.draw();
        });

        delayedResize = false;
    }, 100);
}

$(document).ready(function() {
    $navcontainer = $("#navcontainer");
    $topbar = $("#topbar");
    $grids = $(".fluid_grid");
    $gridclear4 = $(".gridclear_4");
    $gridclear6 = $(".gridclear_6");
    $topsearch = $("#topsearch");
    $navlinks = $(".navrow a");

    $(document).keydown(function(e) {
        if (e.keyCode == 27) {
            HideDatabasesMenu();
        } else if (e.keyCode == 77) {
            ShowDatabasesMenu();
        }
    });

    $topsearch.keyup(function(e) {
        var val = $topsearch.val();

        if (val != "") {
            $navlinks.each(function(i){
                var el = $(this);
                if (el.text().toLowerCase().indexOf(val) >= 0) {
                    el.removeClass("fadeout");
                } else {
                    el.addClass("fadeout");
                }
            });
        } else {
            $navlinks.removeClass("fadeout");
        }
    });

    $(window).resize(function() {
        CheckGridSize();
    });

    $topbar.click(function(e) {
        ShowDatabasesMenu();
    });

    var currentPath = document.location.pathname.substr(1);
    if (currentPath == "") currentPath = "Default view";
    $("#pagetitle").html(currentPath);

    HideDatabasesMenu();
    CheckGridSize();
});

if(/Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent)) {
    // Mobile device stuff!
}