import slugify from 'slugify';
import { PublicationSummaryViewModel } from '../schema';

export const spcPublication = createPublication({
  id: '638482b6-d015-4798-a1a4-e2311253b3e1',
  title: 'Schools, pupils and their characteristics',
});

export const pupilAbsencePublication = createPublication({
  id: '9676af6b-d563-41f4-d071-08da8f468680',
  title: 'Pupil attendance in schools',
});

export const leoPublication = createPublication({
  id: 'b329f8e4-4191-4f0c-66a8-08d9daa3b093',
  title: 'LEO Graduate and Postgraduate Outcomes',
});

export const apprenticeshipsPublication = createPublication({
  id: 'cf0ec981-3583-42a5-b21b-3f2f32008f1b',
  title: 'Apprenticeships and traineeships',
});

// Don't put this in allPublications to keep it hidden
export const benchmarkPublication = createPublication({
  id: '1681557f-510f-446e-bc9a-f2c7a59d1cfa',
  title: 'Benchmarking',
});

export const allPublications: PublicationSummaryViewModel[] = [
  spcPublication,
  pupilAbsencePublication,
  leoPublication,
  apprenticeshipsPublication,
];

function createPublication(
  data: Omit<PublicationSummaryViewModel, 'slug' | '_links'>
): PublicationSummaryViewModel {
  return {
    ...data,
    slug: slugify(data.title.toLowerCase()),
    _links: {
      self: {
        href: `/api/v1/publications/${data.id}`,
      },
      dataSets: {
        href: `/api/v1/publications/${data.id}/data-sets`,
      },
    },
  };
}
