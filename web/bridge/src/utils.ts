export function removeEmptyFields(obj: object): boolean {
  let hasFields = false;
  for (const key in obj) {
    if (obj[key] === null || obj[key] === undefined) {
      delete obj[key];
    } else if (typeof obj[key] === 'object') {
      let foundFields = removeEmptyFields(obj[key]);
      if (!foundFields) {
        delete obj[key];
      } else {
        hasFields = true;
      }
    } else {
      hasFields = true;
    }
  }
  return hasFields;
}
