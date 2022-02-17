import _ from 'lodash';

export function traverseObject(
    object: any,
    visitor: (name: string, value: any, path: string[], object: Object) => void,
    path: string[]): boolean {
  path = path || [];
  return _.forIn(object, function(value: any, name: string) {
    path.push(name);
    if (_.isPlainObject(value)) {
      visitor(name, value, path, object);
      traverseObject(value, visitor, path);
    } else if (
        value === null || value === undefined || _.isString(value) ||
        _.isNumber(value) || _.isBoolean(value)) {
      visitor(name, value, path, object);
    } else if (_.isArray(value)) {
      // TODO: empty arrays, arrays of primities
      visitor(name, value, path, object);
      // for (const idx in value) {
      //   path.push(idx);
      //   traverseObject(value[idx], visitor, path);
      //   path.pop();
      // }
    } else if (value.toJSON) {
      value = value.toJSON();
      visitor(name, value, path, object);
      traverseObject(value, visitor, path);
    }
    path.pop();
  });
}

export function navigateObject(object: any, path: string) {
  let ctx = object;
  for (let name of path.split('.')) {
    ctx = ctx[name];
    if (!ctx) return ctx;
  }
  return ctx;
}

