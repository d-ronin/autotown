
autotown = angular.module('autotown', ['ngRoute']).
    filter('relDate', function() {
        return function(dstr) {
            return moment(dstr).fromNow();
        };
    }).
    filter('agecss', function() {
        return function(dstr) {
            if (moment(dstr).diff(moment(), 'days') < -14) {
                return 'old'
            } else if (moment(dstr).diff(moment()) > 0) {
                return 'unavailable';
            }
            return '';
        };
    }).
    filter('calDate', function() {
        return function(dstr) {
            return moment(dstr).calendar();
        };
    }).
    filter('tomillis', function() {
        return function(seconds) {
            return (seconds * 1000).toFixed(2);
        };
    }).
    config(['$routeProvider', '$locationProvider',
            function($routeProvider, $locationProvider) {
                $locationProvider.html5Mode(true);
                $locationProvider.hashPrefix('!');

                $routeProvider.
                    when('/', {
                        templateUrl: '/static/partials/home.html',
                        controller: 'IndexCtrl'
                    }).
                    when('/tune/:tuna', {
                        templateUrl: '/static/partials/tune.html',
                        controller: 'TuneCtrl'
                    }).
                    when('/crash/', {
                        templateUrl: '/static/partials/crashes.html',
                        controller: 'CrashesCtrl'
                    }).
                    when('/crash/:dummy', {
                        templateUrl: '/static/partials/crash.html',
                        controller: 'CrashCtrl'
                    }).
                    otherwise({
                        redirectTo: '/'
                    });
            }]);

function isBogusTau(t) {
    return t < 0.0075 || t > .3;
}

autotown.controller('IndexCtrl', ['$scope', '$http',
                                  function($scope, $http) {
                                      $scope.isBogusTau = isBogusTau;

                                      $http.get("//dronin-autotown.appspot.com/api/recentTunes").success(function(data) {
                                          $scope.recentTunes = data;
                                          $scope.olderFun = function(d) {
                                              if (!d.older) { return "" }
                                              return d.older.map(function(x) {
                                                  return "" + ((x.tau) * 1000).toFixed(2) + "@" +
                                                      moment(x.timestamp).fromNow();
                                              }).join(', ');
                                          };
                                          setTimeout(function() {
                                              data.forEach(function(d) {
                                                  if (d.older) {
                                                      var rv = d.older.map(function(x) {
                                                          return (x.tau * 1000).toFixed(2);
                                                      });
                                                      rv.reverse();
                                                      rv.push((d.Tau * 1000).toFixed(2));
                                                      jQuery('#spark-' + d.Key).sparkline(rv);
                                                  }
                                              });
                                          }, 1);
                                      });
                                  }]);


autotown.controller('TuneCtrl', ['$scope', '$http', '$routeParams',
                                 function($scope, $http, $routeParams) {
                                     $scope.isBogusTau = isBogusTau;
                                     $scope.olderFun = function(d) {
                                         if (!d) { return "" }
                                         return d.map(function(x) {
                                             return "" + (x.Tau * 1000).toFixed(2) + "@" +
                                                 moment(x.Timestamp).fromNow();
                                         }).join(', ');
                                     };
                                     $scope.rawLink = "//dronin-autotown.appspot.com/api/tune?tune=" +
                                         encodeURIComponent($routeParams.tuna);
                                     $http.get($scope.rawLink).success(function(data) {
                                         $scope.tune = data;
                                         $scope.valid = data.Orig.identification.tau != 0;

                                         $scope.hw = {};
                                         var bkey = "Hw" + data.Board;
                                         if (bkey == "HwCC3D") {
                                             bkey = "HwCopterControl";
                                         }
                                         var board = data.Orig.rawSettings[bkey].fields;
                                         for (var k in board) {
                                             if (k.match(/.*Rate/)) {
                                                 $scope.hw.mpurate = board[k];
                                             } else if (k.match(/.*Accel.*LPF/)) {
                                                 $scope.hw.accellpf = board[k];
                                             } else if (k.match(/.*Gyro.*LPF/)) {
                                                 $scope.hw.gyrolpf = board[k];
                                             } else if (k.match(/.*LPF/)) {
                                                 $scope.hw.accellpf = board[k];
                                                 $scope.hw.gyrolpf = board[k];
                                             }
                                         }

                                         var as = data.Orig.rawSettings.ActuatorSettings.fields;
                                         $scope.bterm = as.MotorInputOutputCurveFit;
                                         if (typeof $scope.bterm === 'object') {
                                             $scope.bterm = $scope.bterm[1];
                                         }
                                     });

                                     var relatedLink = "//dronin-autotown.appspot.com/api/relatedTunes?tune=" +
                                         encodeURIComponent($routeParams.tuna);
                                     $http.get(relatedLink).success(function(data) {
                                         $scope.related = data;
                                         setTimeout(function() {
                                             var rv = data.map(function(d) { return (d.Tau*1000).toFixed(2);});
                                             rv.reverse();
                                             jQuery('#spark-related').sparkline(rv);
                                         }, 1);
                                     })
                                 }]);

autotown.controller('CrashesCtrl', ['$scope', '$http',
                                  function($scope, $http) {
                                      $http.get("//dronin-autotown.appspot.com/api/recentCrashes").success(function(data) {
                                          $scope.recentCrashes = data;
                                          $scope.crashServer = 'https://console.developers.google.com/m/cloudstorage/b/dronin-autotown.appspot.com/o/';
                                      });
                                  }]);

function crashCtrl($scope, $http, $routeParams) {
    $http.get('//dronin-autotown.appspot.com/api/crash/' + $routeParams.dummy).success(function(data) {
        $scope.crash = data;
        $scope.crashServer = 'https://console.developers.google.com/m/cloudstorage/b/dronin-autotown.appspot.com/o/';    });

    $http.get('//dronin-autotown.appspot.com/api/crashtrace/' + $routeParams.dummy).then(function successCallback(response) {
      $scope.sourcecode = {}
      $scope.trace = response.data;

      // fetch gcs source code async
      angular.forEach(response.data.sources, function(relpath) {
        var fetchurl = 'https://api.github.com/repos/d-ronin/dRonin/contents/' + relpath + '?ref=' + response.data.gitrevision;
        $http.get(
          fetchurl,
          {headers: {'Accept': 'application/vnd.github.VERSION.raw'}}
        ).then(function successCallback(response) {
          $scope.sourcecode[relpath] = response.data;
        }, function errorCallback(response) {
          $scope.sourcecode[relpath] = 'Failed to fetch data: ' + response.status + ' ' + response.statusText;
        });
      });
    });
}

autotown.directive('ngPrism',['$interpolate', function ($interpolate) {
  "use strict";
  return {
    restrict: 'E',
    template: '<pre><code ng-transclude></code></pre>',
    replace: true,
    transclude: true,
    link: function (scope, elm) {
      var tmp = $interpolate(elm.find('code').text())(scope);
      elm.find('code').text(tmp);
      elm.attr('data-line', scope.frame.line);
      Prism.highlightElement(elm.find('code')[0], false);
      if (elm.find('div.line-highlight').length < 1)
        return;
      // scroll to highlighted line
      elm.animate({
          scrollTop: elm.find('.line-highlight').position().top - 4/5*elm.height()
      }, 0);
    }
  };
}]);

autotown.controller('CrashCtrl', ['$scope', '$http', '$routeParams', crashCtrl]);
