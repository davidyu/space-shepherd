// Space Shepherd UI logic

var maxdepth = 3;
var showFreeSpace = false;
var userQuota = { total: 1, used: 0, freePerc: "100%" };

function start() {
  treemapContainer = d3.select( "#canvas" )
                       .style( "position", "absolute" )
                       .style( "width", width + "px" )
                       .style( "height", height + "px" )
                       .style( "left", margin.left + "px" )
                       .style( "top", margin.top + "px" );

  d3.json("/get_filetree", function( error, data ) {
    if ( error ) throw error;
    filetree = data.tree;
    userQuota.used = data.used;
    userQuota.total = data.total;
    userQuotaUpdated();
    drawTree( data.tree );

    // poll every three seconds
    window.setInterval( poll, 3000 );
  } );

  // if we haven't deleted the loader in 3 seconds, it means we're
  // probably crawling the tree for the first time (or grabbing a massive delta/
  // update). Show the user what we're up to.
  setTimeout( updateLoadingText, 3000 );

  tooltip = d3.select( "body" )
              .append( "div" )
              .style( "position", "absolute" )
              .style( "z-index", "10" )
              .style( "visibility", "hidden" )
              .attr( "class", "d3-tip" )
}

// http://stackoverflow.com/a/14919494
// converts bytes into the most reasonable measure of size
function humanFileSize( bytes, si ) {
    var thresh = si ? 1000 : 1024;
    if ( Math.abs( bytes ) < thresh ) {
        return bytes + ' B';
    }
    var units = [ 'kB','MB','GB','TB','PB','EB','ZB','YB' ];
    var u = -1;
    do {
        bytes /= thresh;
        ++u;
    } while ( Math.abs( bytes ) >= thresh && u < units.length - 1 );
    return bytes.toFixed( 1 ) + ' ' + units[u];
}

// http://stackoverflow.com/a/2035211
// returns the browser client viewport
function getViewport() {
    var viewPortWidth;
    var viewPortHeight;

    if (typeof window.innerWidth != 'undefined') {
        viewPortWidth = window.innerWidth,
        viewPortHeight = window.innerHeight
    } else if ( typeof document.documentElement != 'undefined' &&
                typeof document.documentElement.clientWidth != 'undefined' &&
                document.documentElement.clientWidth != 0 ) {
        // IE6 in standards compliant mode
        viewPortWidth = document.documentElement.clientWidth,
        viewPortHeight = document.documentElement.clientHeight
    } else {
        // le sigh...
        viewPortWidth = document.getElementsByTagName('body')[0].clientWidth,
        viewPortHeight = document.getElementsByTagName('body')[0].clientHeight
    }
    return { w: viewPortWidth, h: viewPortHeight };
}

var viewport = getViewport();

var margin = {top: 66, right: 20, bottom: 20, left: 20};
var width  = viewport.w - margin.left - margin.right;
var height = viewport.h - margin.top - margin.bottom;

// generated via http://colorbrewer2.org/ 
// scale for file and folder backgrounds
var color = d3.scale.ordinal().range( [ '#fbb4ae'
                                      , '#b3cde3'
                                      , '#ccebc5'
                                      , '#decbe4'
                                      , '#fed9a6'
                                      , '#ffffcc'
                                      , '#8dd3c7'
                                      , '#ffffb3'
                                      , '#bebada'
                                      , '#fb8072'
                                      , '#80b1d3'
                                      , '#fdb462'
                                      , '#b3de69'
                                      , '#fccde5'
                                      , '#d9d9d9'
                                      , '#bc80bd'
                                      , '#ccebc5'
                                      , '#ffed6f' ] );

// main reference for our treemap
var treemap = d3.layout.treemap()
                .size( [ width, height ] )
                .padding( [ 15, 5, 5, 5 ] )
                .sort( function( a,b ) { return a.size - b.size; } )
                .value( function( d ) { return d.size; } );

var filetree = null;
var treemapContainer = null;
var tooltip = null;

// dummy node that designates free space
var freeNode  = { "name": "Free space"
                , "path": "FREESPACE"
                , "size": 0
                };

function percentageOfOccupiedSpace( node ) {
  return ( node.size * 100 / filetree.size ).toFixed( 1 ) + "% of total occupied space";
}

function percentageOfQuota( node ) {
  return ( node.size * 100 / userQuota.total ).toFixed( 1 ) + "% of your quota";
}

function isRoot( node ) {
  return node.name == '/';
}

function updateAndShowToolTip(node) {
  if ( node == freeNode ) {
    return tooltip.html( "Free space (" + userQuota.freePerc + " of your quota)" )
                  .style( "visibility", "visible" );
  } else {
    return tooltip.html( node.name + "<br/>" +
                         ( node.is_dir ? "Folder" + ( isRoot( node ) ? " (root)" : "" ) + "<br/>" : "" ) +
                         humanFileSize( node.size ) + "<br/>" +
                         ( isRoot( node ) ? "" : node.path + "<br/>" ) + 
                         ( isRoot( node ) ? "" : showFreeSpace ? percentageOfQuota( node ) : percentageOfOccupiedSpace( node ) ) + "<br/>"
                        )
                  .style( "visibility", "visible" );
  }
}

// make sure the tooltip is never flush with the viewport border

var TITLESAFE_MARGIN = 10;
var TIP_OFFSET_X = 20;
var TIP_OFFSET_Y = 20;

function moveToolTip() {
  var x = d3.event.pageX;
  var y = d3.event.pageY;
  var w = tooltip.node().offsetWidth;
  var h = tooltip.node().offsetHeight;
  if ( x + TIP_OFFSET_X + TITLESAFE_MARGIN > viewport.w - w || y + TIP_OFFSET_Y + TITLESAFE_MARGIN > viewport.h - h ) {
    return tooltip.style( "top" , ( y - h + TIP_OFFSET_Y ) + "px")
                  .style( "left", ( x - w - TIP_OFFSET_X ) + "px");
  } else {
    return tooltip.style( "top" , ( y - TIP_OFFSET_Y ) + "px")
                  .style( "left", ( x + TIP_OFFSET_X ) + "px");
  }
}

function hideToolTip() {
 return tooltip.style( "visibility", "hidden" );
}

function updateLoadingText() {
  var caption = document.getElementById( "loader-caption" );
  if ( caption ) {
    caption.innerHTML += "<p>Hang tight, we're crawling your file tree for the first time. This could take a couple of minutes...</p>"
  }
}

function position() {
  this.style( "left"   , function( d ) { return d.x + "px"; } )
      .style( "top"    , function( d ) { return d.y + "px"; } )
      .style( "width"  , function( d ) { return Math.max( 0, d.dx - 1 ) + "px"; } )
      .style( "height" , function( d ) { return Math.max( 0, d.dy - 1 ) + "px"; } );
}

function tooSmall( node ) {
  return node.dx < 3 || node.dy < 3 || node.dx * node.dy < 10;
}

function enoughRoomForFileName( node ) {
  return node.dx > 30 && node.dy > 18;
}

function computeClass( node ) {
  if ( tooSmall(node) || node.depth > maxdepth ) {
    return "hidden";
  } else if ( node.is_dir ) {
    return "folder node"
  } else if ( node == freeNode ) {
    return "free node";
  } else {
    return "node";
  }
}

function labelNode( node ) {
  if ( node == freeNode ) {
    return "<span class='free-label'>(Free space: " +  userQuota.freePerc + ")<br/>" + humanFileSize( node.size ) + "</span>";
  } else {
    return enoughRoomForFileName( node ) ? node.name : null;
  }
}

function drawTree( root ) {
  var node = treemapContainer.datum( root )
                             .selectAll( ".node" )
                             .data( treemap.nodes, function( d ) { return d.path; } );

  // delete the loader element since we're done loading
  var loader = document.getElementById( "loader" );
  loader.parentNode.removeChild( loader );

  // we only care about enter() as we are only adding new nodes when we draw
  // the tree for the first time
  node.enter()
      .append( "div" )
      .attr( "class", computeClass )
      .call(position)
      .style( "background", function( d ) { return color( d.depth ); } )
      .html( labelNode )
      .on( "mouseover", updateAndShowToolTip )
      .on( "mousemove", moveToolTip )
      .on( "mouseout", hideToolTip );

  window.addEventListener( 'resize', resizeAfterDelay );
}

// we want to resize at the end or after the user hasn't touched the window in a while
// this is to prevent calls to updateTree multiple times per second, which is excessive
// and potentially expensive
var queuedResizeId;
function resizeAfterDelay(e) {
  clearTimeout(queuedResizeId);
  queuedResizeId = setTimeout(resizeWindow, 500);
}

function resizeWindow() {
  viewport = getViewport();
  width  = viewport.w - margin.left - margin.right,
  height = viewport.h - margin.top - margin.bottom;

  treemapContainer.style("width", width + "px")
                  .style("height", height + "px");

  treemap.size( [ width, height ] );
  updateTree( filetree );
}

// update the treemap with new data
function updateTree(root) {
  var node = treemapContainer.datum( root )
                             .selectAll(".node")
                             .data( treemap.nodes, function( d ) { return d.path; } );

  node.enter()
      .append( "div" )
      .attr( "class", computeClass )
      .call( position )
      .style( "background", function( d ) { return color(d.depth); } )
      .html( labelNode )
      .on( "mouseover", updateAndShowToolTip )
      .on( "mousemove", moveToolTip )
      .on( "mouseout", hideToolTip );

  node.exit()
      .remove();

  // transition nodes just need to update their position (as their sizes may have changed)
  // and their labels
  node.attr( "class", computeClass )
      .call( position )
      .html( labelNode )
}

function userQuotaUpdated() {
  userQuota.freePerc = ( Math.max(1 - userQuota.used / userQuota.total, 0 ) * 100 ).toFixed(1) + "%";
  var usage = Math.min( userQuota.used / userQuota.total, 1 );
  var freeSpaceToggle = document.getElementById( "free-space-toggle-button" );

  if ( usage < 1 ) {
    freeSpaceToggle.className = "";
  } else {
    freeSpaceToggle.className = "hidden";
  }

  freeNode.size = Math.max( 0, userQuota.total - userQuota.used );
}

var waitingForPollResponse = false;
function poll() {
  if ( waitingForPollResponse ) return;
  waitingForPollResponse = true;
  d3.json( "/update_filetree", function( error, data ) {
    waitingForPollResponse = false;
    if ( error ) throw error;
    if ( data.changed ) {
      if ( showFreeSpace ) {
        augmentTreeWithFreeNode( data.tree );
      } else {
        deleteFreeNodeFromTree( data.tree );
      }
      filetree = data.tree;
      userQuota.used = data.used;
      userQuota.total = data.total;
      userQuotaUpdated();
      updateTree( data.tree );
    }
  } );
}

// toggles the free space node visualization
function toggleFreeSpace() {
  showFreeSpace = !showFreeSpace;
  var toggleButton = document.getElementById( "free-space-toggle-button" );
  if (showFreeSpace) {
    augmentTreeWithFreeNode( filetree );
    toggleButton.innerHTML = "Hide free space";
  } else {
    deleteFreeNodeFromTree( filetree );
    toggleButton.innerHTML = "Show free space";
  }
  updateTree( filetree );
}

function augmentTreeWithFreeNode( tree ) {
  if ( tree.children[ tree.children.length-1 ] != freeNode ) {
    tree.children.push( freeNode );
  }
}

function deleteFreeNodeFromTree( tree ) {
  // by construction, the last child of the root node is the free node, if it exists
  if ( tree.children[ tree.children.length-1 ] == freeNode ) {
    tree.children.pop();
  }
}
